from __future__ import annotations

import json
import math
import re
import tempfile
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import sys

import numpy as np
import pandas as pd
import pgeocode
import pydeck as pdk
import streamlit as st
from PIL import Image
import altair as alt

REPO_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = REPO_ROOT.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
SCRAPER_ROOT = PROJECT_ROOT / "OM_Scraper"
if SCRAPER_ROOT.exists() and str(SCRAPER_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRAPER_ROOT))

from app.lib.supabase_io import (
    fetch_crexi,
    supabase_ping,
    fetch_crexi_with_fallback,
    fetch_market_medians,
    fetch_realtor_props,
    fetch_realtor_props_with_fallback,
    fetch_realtor_rent,
)
from app.gwr.pipeline import gwr_surface, run_teammate_gwr
from om_extractor import (
    call_azure_extraction,
    images_to_base64,
    pdf_to_images,
)
DEFAULT_PDF_FOLDER = (REPO_ROOT / ".." / "OMs").resolve()

EARTH_RADIUS_MILES = 3958.7613
VACANCY_DEFAULT = 0.06

MISSING_STRINGS = {"", "na", "n/a", "none", "null", "-", "--"}

_GEOCODER_WARNING_SHOWN = False


@st.cache_resource(show_spinner=False)
def get_geocoder() -> Optional[pgeocode.Nominatim]:
    try:
        return pgeocode.Nominatim("us")
    except Exception:
        return None

BEDROOM_MAP = [
    ("1 Bed", "1_bed", 1),
    ("2 Bed", "2_bed", 2),
    ("3 Bed", "3_bed", 3),
    ("4 Bed", "4_bed", 4),
]

TAB_LABELS = [
    "Overview",
    "Rent vs Market",
    "Financials",
    "Spatial Pricing",
    "Comps",
    "Raw JSON",
    "PDF Preview",
]
ACTIVE_TAB_KEY = "comparison_active_tab"


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        lowered = cleaned.lower()
        if lowered in MISSING_STRINGS:
            return None
        cleaned = cleaned.replace(",", "")
        match = re.search(r"[-+]?\d*\.?\d+", cleaned)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None
    return None


def to_int(value: Any) -> Optional[int]:
    number = to_float(value)
    if number is None:
        return None
    if math.isnan(number):
        return None
    return int(round(number))


def to_percent(value: Any) -> Optional[float]:
    number = to_float(value)
    if number is None:
        return None
    if number > 1.0:
        return number / 100.0
    return number


def parse_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return bool(int(value))
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if not cleaned or cleaned in MISSING_STRINGS:
            return None
        if cleaned in {"yes", "true", "1", "y"}:
            return True
        if cleaned in {"no", "false", "0", "n"}:
            return False
    return None


def format_money(value: Optional[float], decimals: int = 0) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    if decimals > 0:
        formatted = f"{value:,.{decimals}f}"
    else:
        formatted = f"{value:,.0f}"
    return f"${formatted}"


def format_number(value: Optional[float], decimals: int = 0) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    if decimals > 0:
        return f"{value:,.{decimals}f}"
    return f"{value:,.0f}"


def format_percent(value: Optional[float], decimals: int = 1) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "-"
    return f"{value * 100:.{decimals}f}%"


def pct_diff(value: Optional[float], reference: Optional[float]) -> Optional[float]:
    if value is None or reference is None:
        return None
    if reference == 0:
        return None
    return (value - reference) / reference


def remember_tab(tab_name: str) -> None:
    st.session_state[ACTIVE_TAB_KEY] = tab_name


FORMATTERS = {
    "money0": lambda v: format_money(v, 0),
    "money1": lambda v: format_money(v, 1),
    "money2": lambda v: format_money(v, 2),
    "number0": lambda v: format_number(v, 0),
    "number1": lambda v: format_number(v, 1),
    "number2": lambda v: format_number(v, 2),
    "percent1": lambda v: format_percent(v, 1),
    "percent2": lambda v: format_percent(v, 2),
}


def parse_address_components(address: Optional[str]) -> Dict[str, Optional[str]]:
    if not address or not isinstance(address, str):
        return {"street": None, "city": None, "state": None, "zip_code": None}
    parts = [p.strip() for p in address.split(",") if p.strip()]
    street = None
    city = None
    state = None
    zip_code = None
    state_zip = ""

    if len(parts) >= 3:
        street = ", ".join(parts[:-2])
        city = parts[-2]
        state_zip = parts[-1]
    elif len(parts) == 2:
        street = parts[0]
        state_zip = parts[1]
    elif parts:
        street = parts[0]
        state_zip = parts[-1]

    state_match = re.search(r"\b([A-Za-z]{2})\b", state_zip)
    if state_match:
        state = state_match.group(1).upper()

    zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", address)
    if zip_match:
        zip_code = zip_match.group(1)

    return {
        "street": street,
        "city": city,
        "state": state,
        "zip_code": zip_code,
    }


def parse_lot_size_to_acres(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    if isinstance(value, str):
        number = to_float(value)
        if number is None:
            return None
        lower = value.lower()
        if "sf" in lower or "sqft" in lower or "square" in lower or "sq ft" in lower:
            return number / 43560.0
        if "acre" in lower or "ac" in lower:
            return number
        return number
    return None


@st.cache_data(show_spinner=False)
def geocode_zip(zip_code: str) -> Optional[Tuple[float, float]]:
    global _GEOCODER_WARNING_SHOWN
    if not zip_code:
        return None
    geocoder = get_geocoder()
    if geocoder is None:
        if not _GEOCODER_WARNING_SHOWN:
            st.warning("Postal code lookup unavailable; distance calculations will rely on city/state only.")
            _GEOCODER_WARNING_SHOWN = True
        return None
    try:
        result = geocoder.query_postal_code(zip_code)
    except Exception:
        if not _GEOCODER_WARNING_SHOWN:
            st.warning("Postal code lookup failed; distance calculations will rely on city/state only.")
            _GEOCODER_WARNING_SHOWN = True
        return None
    if result is None:
        return None
    lat = result["latitude"] if "latitude" in result else getattr(result, "latitude", None)
    lon = result["longitude"] if "longitude" in result else getattr(result, "longitude", None)
    if pd.isna(lat) or pd.isna(lon):
        return None
    return float(lat), float(lon)


def add_distance_column(
    df: pd.DataFrame,
    origin: Optional[Tuple[float, float]],
    lat_col: str,
    lon_col: str,
) -> pd.DataFrame:
    df = df.copy()
    if origin is None:
        df["distance_miles"] = np.nan
        return df

    lat = pd.to_numeric(df[lat_col], errors="coerce")
    lon = pd.to_numeric(df[lon_col], errors="coerce")
    mask = lat.notna() & lon.notna()

    df["distance_miles"] = np.nan
    if mask.any():
        lat_rad = np.radians(lat[mask])
        lon_rad = np.radians(lon[mask])
        origin_lat = math.radians(origin[0])
        origin_lon = math.radians(origin[1])
        dlat = lat_rad - origin_lat
        dlon = lon_rad - origin_lon
        a = np.sin(dlat / 2.0) ** 2 + np.cos(origin_lat) * np.cos(lat_rad) * np.sin(dlon / 2.0) ** 2
        c = 2 * np.arcsin(np.sqrt(a))
        df.loc[mask, "distance_miles"] = EARTH_RADIUS_MILES * c

    return df


def filter_by_distance(
    df: pd.DataFrame,
    profile: Dict[str, Any],
    max_distance: float,
    origin: Optional[Tuple[float, float]],
    lat_col: str,
    lon_col: str,
    city_col: str = "city_upper",
    state_col: str = "state",
) -> pd.DataFrame:
    df = add_distance_column(df, origin, lat_col, lon_col)
    if origin is not None:
        df = df[(df["distance_miles"].isna()) | (df["distance_miles"] <= max_distance)]
    else:
        if profile.get("city_upper") and city_col in df.columns:
            df = df[df[city_col] == profile["city_upper"]]
        if profile.get("state") and state_col in df.columns:
            df = df[df[state_col].str.upper() == profile["state"]]
    return df


def build_property_profile(om_data: Dict[str, Any]) -> Dict[str, Any]:
    location = om_data.get("location_data", {}) or {}
    financials = om_data.get("financials", {}) or {}
    unit_info_raw = om_data.get("unit_info", {}) or {}
    summary = om_data.get("summary", "")

    address = location.get("address")
    address_parts = parse_address_components(address)
    if address_parts.get("city"):
        st.session_state["om_city"] = address_parts["city"]
    if address_parts.get("state"):
        st.session_state["om_state"] = address_parts["state"]
    lot_size_raw = location.get("lot_size")
    lot_size_acres = parse_lot_size_to_acres(lot_size_raw)

    rentable_sf = to_float(location.get("rentable_square_footage"))
    total_units = to_int(location.get("total_units"))
    property_age = to_int(location.get("property_age"))
    year_renovated = to_int(location.get("year_renovated"))
    oz_status = parse_bool(location.get("oz_status"))

    asking_price = to_float(financials.get("asking_price"))
    noi = to_float(financials.get("noi"))
    cap_rate = to_percent(financials.get("cap_rate"))
    expense_ratio = to_percent(financials.get("expense_ratio"))
    expense_cost = to_float(financials.get("expense_cost"))

    unit_info: Dict[str, Dict[str, Optional[float]]] = {}
    for label, key, beds in BEDROOM_MAP:
        raw = unit_info_raw.get(key, {}) or {}
        unit_info[key] = {
            "label": label,
            "beds": beds,
            "number_of_units": to_int(raw.get("number_of_units")),
            "average_rent": to_float(raw.get("average_rent")),
            "average_sqft": to_float(raw.get("average_sqft")),
        }

    price_per_unit = None
    if asking_price is not None and total_units:
        if total_units != 0:
            price_per_unit = asking_price / total_units

    price_per_sqft = None
    if asking_price is not None and rentable_sf:
        if rentable_sf != 0:
            price_per_sqft = asking_price / rentable_sf

    price_per_acre = None
    if asking_price is not None and lot_size_acres:
        if lot_size_acres != 0:
            price_per_acre = asking_price / lot_size_acres

    profile = {
        "address": address,
        "street": address_parts.get("street"),
        "city": address_parts.get("city"),
        "city_upper": address_parts.get("city").upper() if address_parts.get("city") else None,
        "state": address_parts.get("state"),
        "zip_code": address_parts.get("zip_code"),
        "lot_size_raw": lot_size_raw,
        "lot_size_acres": lot_size_acres,
        "rentable_square_footage": rentable_sf,
        "total_units": total_units,
        "property_age": property_age,
        "year_renovated": year_renovated,
        "oz_status": oz_status,
        "asking_price": asking_price,
        "noi": noi,
        "cap_rate": cap_rate,
        "expense_ratio": expense_ratio,
        "expense_cost": expense_cost,
        "price_per_unit": price_per_unit,
        "price_per_sqft": price_per_sqft,
        "price_per_acre": price_per_acre,
        "unit_info": unit_info,
        "summary": summary,
    }
    return profile


def resolve_property_coordinates(profile: Dict[str, Any], datasets: List[Optional[pd.DataFrame]]) -> Optional[Tuple[float, float]]:
    zip_code = profile.get("zip_code")
    if zip_code:
        coords = geocode_zip(zip_code)
        if coords:
            return coords

    city_upper = profile.get("city_upper")
    if not city_upper:
        return None

    for df in datasets:
        if df is None:
            continue
        required = {"city_upper", "latitude", "longitude"}
        if not required.issubset(df.columns):
            continue
        subset = df.loc[
            (df["city_upper"] == city_upper) & df["latitude"].notna() & df["longitude"].notna(),
            ["latitude", "longitude"],
        ]
        if not subset.empty:
            lat = float(pd.to_numeric(subset["latitude"], errors="coerce").mean())
            lon = float(pd.to_numeric(subset["longitude"], errors="coerce").mean())
            if not math.isnan(lat) and not math.isnan(lon):
                return lat, lon
    return None


@st.cache_data(show_spinner=False)
def load_crexi_dataset(
    city: Optional[str],
    state: Optional[str],
    limit: int = 1000,
) -> pd.DataFrame:
    city_filter = city.title().strip() if isinstance(city, str) and city.strip() else None
    state_filter = state.upper().strip() if isinstance(state, str) and state.strip() else None
    df = fetch_crexi(city=city_filter, state=state_filter, limit=limit)
    if df.empty:
        return df

    df = df.copy()
    rename_variants = {
        "property_link": ["Property Link", "property_link"],
        "property_name": ["Property Name", "property_name"],
        "property_status": ["Property Status", "property_status"],
        "property_type": ["Type", "property_type"],
        "address": ["Address", "address"],
        "city": ["City", "city"],
        "state": ["State", "state"],
        "zip_code": ["Zip", "zip_code"],
        "sqft": ["SqFt", "sqft"],
        "lot_size_acres": ["Lot Size", "lot_size_acres"],
        "units": ["Units", "units"],
        "price_per_unit": ["Price/Unit", "price_per_unit"],
        "noi": ["NOI", "noi"],
        "cap_rate": ["Cap Rate", "cap_rate"],
        "asking_price": ["Asking Price", "asking_price"],
        "price_per_sqft": ["Price/SqFt", "price_per_sqft"],
        "price_per_acre": ["Price/Acre", "price_per_acre"],
        "opportunity_zone": ["Opportunity Zone", "opportunity_zone"],
        "longitude": ["Longitude", "longitude"],
        "latitude": ["Latitude", "latitude"],
    }
    rename_map = {}
    for target, variants in rename_variants.items():
        for variant in variants:
            if variant in df.columns and variant != target:
                rename_map[variant] = target
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    numeric_cols = [
        "sqft",
        "lot_size_acres",
        "units",
        "price_per_unit",
        "noi",
        "cap_rate",
        "asking_price",
        "price_per_sqft",
        "price_per_acre",
        "longitude",
        "latitude",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "cap_rate" in df.columns:
        df["cap_rate"] = df["cap_rate"].apply(lambda x: x / 100.0 if pd.notna(x) and x > 1 else x)

    if {"price_per_unit", "asking_price", "units"}.issubset(df.columns):
        mask = (
            df["price_per_unit"].isna()
            & df["asking_price"].notna()
            & df["units"].notna()
            & (df["units"] != 0)
        )
        df.loc[mask, "price_per_unit"] = df.loc[mask, "asking_price"] / df.loc[mask, "units"]

    if {"price_per_sqft", "asking_price", "sqft"}.issubset(df.columns):
        mask = (
            df["price_per_sqft"].isna()
            & df["asking_price"].notna()
            & df["sqft"].notna()
            & (df["sqft"] != 0)
        )
        df.loc[mask, "price_per_sqft"] = df.loc[mask, "asking_price"] / df.loc[mask, "sqft"]

    if {"price_per_acre", "asking_price", "lot_size_acres"}.issubset(df.columns):
        mask = (
            df["price_per_acre"].isna()
            & df["asking_price"].notna()
            & df["lot_size_acres"].notna()
            & (df["lot_size_acres"] != 0)
        )
        df.loc[mask, "price_per_acre"] = df.loc[mask, "asking_price"] / df.loc[mask, "lot_size_acres"]

    if "opportunity_zone" in df.columns:
        df["opportunity_zone_bool"] = df["opportunity_zone"].apply(parse_bool)
    else:
        df["opportunity_zone_bool"] = np.nan

    if "city" in df.columns:
        df["city"] = df["city"].astype(str).str.strip()
        df["city_upper"] = df["city"].str.upper()
    else:
        df["city_upper"] = None

    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.upper().str.strip()

    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).str.strip()

    for col in ["latitude", "longitude"]:
        if col not in df.columns:
            df[col] = np.nan

    return df

@st.cache_data(show_spinner=False)
def load_realtor_sale_dataset(
    city: Optional[str],
    state: Optional[str],
    limit: int = 5000,
) -> pd.DataFrame:
    city_filter = city.lower().strip() if isinstance(city, str) and city.strip() else None
    state_filter = state.lower().strip() if isinstance(state, str) and state.strip() else None
    df = fetch_realtor_props(city=city_filter, state=state_filter, limit=limit)
    if df.empty:
        return df

    df = df.copy()
    rename_variants = {
        "property_url": ["property_url"],
        "formatted_address": ["formatted_address", "address"],
        "city": ["city"],
        "state": ["state"],
        "zip_code": ["zip_code", "postal_code"],
        "status": ["status"],
        "beds": ["beds"],
        "full_baths": ["full_baths"],
        "sqft": ["sqft"],
        "year_built": ["year_built"],
        "list_price": ["list_price", "price"],
        "lot_sqft": ["lot_sqft"],
        "price_per_sqft": ["price_per_sqft"],
        "latitude": ["latitude"],
        "longitude": ["longitude"],
        "stories": ["stories"],
        "hoa_fee": ["hoa_fee"],
    }
    rename_map = {}
    for target, variants in rename_variants.items():
        for variant in variants:
            if variant in df.columns and variant != target:
                rename_map[variant] = target
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    cols = [
        "property_url",
        "formatted_address",
        "city",
        "state",
        "zip_code",
        "status",
        "beds",
        "full_baths",
        "sqft",
        "year_built",
        "list_price",
        "lot_sqft",
        "price_per_sqft",
        "latitude",
        "longitude",
        "stories",
        "hoa_fee",
    ]
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols].copy()

    df = df.rename(columns={"formatted_address": "address"})

    numeric_cols = [
        "beds",
        "full_baths",
        "sqft",
        "year_built",
        "list_price",
        "lot_sqft",
        "price_per_sqft",
        "latitude",
        "longitude",
        "stories",
        "hoa_fee",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "lot_sqft" in df.columns:
        df["lot_acres"] = df["lot_sqft"] / 43560.0
    else:
        df["lot_acres"] = np.nan

    if "list_price" in df.columns:
        df["price_per_acre"] = np.where(
            (df["list_price"].notna()) & (df["lot_acres"].notna()) & (df["lot_acres"] != 0),
            df["list_price"] / df["lot_acres"],
            np.nan,
        )
    else:
        df["price_per_acre"] = np.nan

    df["city"] = df.get("city", pd.Series(dtype=str)).astype(str).str.strip()
    df["city_upper"] = df["city"].str.upper()
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.upper().str.strip()
    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).str.strip()

    for col in ["latitude", "longitude"]:
        if col not in df.columns:
            df[col] = np.nan

    return df


@st.cache_data(show_spinner=False)
def load_realtor_rent_dataset(
    city: Optional[str],
    state: Optional[str],
    limit: int = 5000,
) -> pd.DataFrame:
    city_filter = city.lower().strip() if isinstance(city, str) and city.strip() else None
    state_filter = state.lower().strip() if isinstance(state, str) and state.strip() else None
    df = fetch_realtor_rent(city=city_filter, state=state_filter, limit=limit)
    if df.empty:
        return df

    df = df.copy()
    rename_variants = {
        "property_url": ["property_url"],
        "formatted_address": ["formatted_address", "address"],
        "city": ["city"],
        "state": ["state"],
        "zip_code": ["zip_code", "postal_code"],
        "status": ["status"],
        "beds": ["beds"],
        "sqft": ["sqft"],
        "list_price": ["list_price", "asking_rent"],
        "latitude": ["latitude"],
        "longitude": ["longitude"],
    }
    rename_map = {}
    for target, variants in rename_variants.items():
        for variant in variants:
            if variant in df.columns and variant != target:
                rename_map[variant] = target
                break
    if rename_map:
        df = df.rename(columns=rename_map)

    cols = [
        "property_url",
        "formatted_address",
        "city",
        "state",
        "zip_code",
        "status",
        "beds",
        "sqft",
        "list_price",
        "latitude",
        "longitude",
    ]
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols].copy()

    df = df.rename(columns={"formatted_address": "address", "list_price": "asking_rent"})

    numeric_cols = ["beds", "sqft", "asking_rent", "latitude", "longitude"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["city"] = df.get("city", pd.Series(dtype=str)).astype(str).str.strip()
    df["city_upper"] = df["city"].str.upper()
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.upper().str.strip()
    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).str.strip()

    for col in ["latitude", "longitude"]:
        if col not in df.columns:
            df[col] = np.nan

    return df


def apply_crexi_filters(
    df: Optional[pd.DataFrame],
    profile: Dict[str, Any],
    filters: Dict[str, float],
    origin: Optional[Tuple[float, float]],
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    filtered = filter_by_distance(df, profile, filters["max_distance"], origin, "latitude", "longitude")

    if profile.get("total_units") is not None and filters.get("max_units_diff") is not None:
        om_units = profile["total_units"]
        mask = filtered["units"].notna()
        filtered = filtered[~mask | (np.abs(filtered["units"] - om_units) <= filters["max_units_diff"])]

    if profile.get("asking_price") and filters.get("max_price_pct") is not None and profile["asking_price"] != 0:
        om_price = profile["asking_price"]
        mask = filtered["asking_price"].notna()
        filtered = filtered[~mask | (np.abs(filtered["asking_price"] - om_price) / om_price <= filters["max_price_pct"])]

    if profile.get("cap_rate") is not None and filters.get("max_cap_diff") is not None:
        om_cap = profile["cap_rate"]
        mask = filtered["cap_rate"].notna()
        filtered = filtered[~mask | (np.abs(filtered["cap_rate"] - om_cap) <= filters["max_cap_diff"])]

    if profile.get("price_per_unit") and filters.get("max_ppu_pct") is not None and profile["price_per_unit"] != 0:
        om_ppu = profile["price_per_unit"]
        mask = filtered["price_per_unit"].notna()
        filtered = filtered[~mask | (np.abs(filtered["price_per_unit"] - om_ppu) / om_ppu <= filters["max_ppu_pct"])]

    if profile.get("price_per_sqft") and filters.get("max_ppsf_pct") is not None and profile["price_per_sqft"] != 0:
        om_ppsf = profile["price_per_sqft"]
        mask = filtered["price_per_sqft"].notna()
        filtered = filtered[~mask | (np.abs(filtered["price_per_sqft"] - om_ppsf) / om_ppsf <= filters["max_ppsf_pct"])]

    if profile.get("price_per_acre") and filters.get("max_ppacre_pct") is not None and profile["price_per_acre"] != 0:
        om_ppa = profile["price_per_acre"]
        mask = filtered["price_per_acre"].notna()
        filtered = filtered[~mask | (np.abs(filtered["price_per_acre"] - om_ppa) / om_ppa <= filters["max_ppacre_pct"])]

    return filtered


def filter_realtor_sales(
    df: Optional[pd.DataFrame],
    profile: Dict[str, Any],
    filters: Dict[str, float],
    origin: Optional[Tuple[float, float]],
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    filtered = filter_by_distance(df, profile, filters["max_distance"], origin, "latitude", "longitude")

    if profile.get("asking_price") and filters.get("max_price_pct") is not None and profile["asking_price"] != 0:
        om_price = profile["asking_price"]
        if "list_price" in filtered.columns:
            mask = filtered["list_price"].notna()
            filtered = filtered[~mask | (np.abs(filtered["list_price"] - om_price) / om_price <= filters["max_price_pct"])]

    return filtered


def filter_realtor_rents(
    df: Optional[pd.DataFrame],
    profile: Dict[str, Any],
    filters: Dict[str, float],
    origin: Optional[Tuple[float, float]],
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    return filter_by_distance(df, profile, filters["max_distance"], origin, "latitude", "longitude")

def build_rent_comparison(
    unit_info: Dict[str, Dict[str, Optional[float]]],
    rent_df: pd.DataFrame,
    vacancy_rate: float,
) -> Tuple[pd.DataFrame, Dict[str, Optional[float]]]:
    rows: List[Dict[str, Any]] = []
    total_units = 0
    weighted_om = 0.0
    weighted_market = 0.0
    total_gpr_om = 0.0
    total_gpr_market = 0.0

    for label, key, bed_count in BEDROOM_MAP:
        info = unit_info.get(key, {}) or {}
        om_units = info.get("number_of_units")
        om_rent = info.get("average_rent")

        subset = rent_df
        if not subset.empty and "beds" in subset.columns:
            subset = subset[subset["beds"].notna()]
            subset = subset[np.isclose(subset["beds"], bed_count, atol=0.25)]
        else:
            subset = pd.DataFrame()

        market_avg = subset["asking_rent"].mean() if not subset.empty else None
        market_median = subset["asking_rent"].median() if not subset.empty else None
        sample_count = int(subset.shape[0]) if not subset.empty else 0
        avg_distance = float(subset["distance_miles"].mean()) if not subset.empty and "distance_miles" in subset.columns else None

        if om_units:
            total_units += om_units
            if om_rent is not None:
                weighted_om += om_units * om_rent
                total_gpr_om += om_units * om_rent * 12
            if market_avg is not None:
                weighted_market += om_units * market_avg
                total_gpr_market += om_units * market_avg * 12

        rent_delta_pct = pct_diff(om_rent, market_avg)

        rows.append(
            {
                "Unit Type": label,
                "OM Units": om_units,
                "OM Avg Rent": om_rent,
                "Market Avg Rent": market_avg,
                "Market Median Rent": market_median,
                "Rent Delta %": rent_delta_pct,
                "OM GPR": om_units * om_rent * 12 if om_units and om_rent is not None else None,
                "Market GPR": om_units * market_avg * 12 if om_units and market_avg is not None else None,
                "Sample Size": sample_count,
                "Avg Distance (mi)": avg_distance,
            }
        )

    weighted_om_avg = (weighted_om / total_units) if total_units and weighted_om else None
    weighted_market_avg = (weighted_market / total_units) if total_units and weighted_market else None
    rent_delta_total = pct_diff(weighted_om_avg, weighted_market_avg)
    egi_om = total_gpr_om * (1 - vacancy_rate) if total_gpr_om else None
    egi_market = total_gpr_market * (1 - vacancy_rate) if total_gpr_market else None

    totals = {
        "total_units": total_units,
        "weighted_om_avg": weighted_om_avg,
        "weighted_market_avg": weighted_market_avg,
        "rent_delta_total": rent_delta_total,
        "gpr_om": total_gpr_om if total_gpr_om else None,
        "gpr_market": total_gpr_market if total_gpr_market else None,
        "egi_om": egi_om,
        "egi_market": egi_market,
        "vacancy_rate": vacancy_rate,
    }

    rent_table = pd.DataFrame(rows)
    return rent_table, totals


def style_rent_table(df: pd.DataFrame) -> "pd.io.formats.style.Styler":
    def fmt_units(val: Any) -> str:
        return format_number(val, 0)

    def fmt_money0(val: Any) -> str:
        return format_money(val, 0)

    def fmt_distance(val: Any) -> str:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return "-"
        return f"{val:.1f}"

    def fmt_count(val: Any) -> str:
        return format_number(val, 0)

    styler = (
        df.style.format({
            "OM Units": fmt_units,
            "OM Avg Rent": fmt_money0,
            "Market Avg Rent": fmt_money0,
            "Market Median Rent": fmt_money0,
            "OM GPR": fmt_money0,
            "Market GPR": fmt_money0,
            "Sample Size": fmt_count,
            "Avg Distance (mi)": fmt_distance,
            "Rent Delta %": lambda v: format_percent(v, 1),
        })
    )

    def colorize(val: Any) -> str:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return ""
        absolute = abs(val)
        if absolute <= 0.05:
            return "background-color:#c8e6c9;"
        if absolute <= 0.10:
            return "background-color:#ffe0b2;"
        return "background-color:#ffcdd2;"

    styler = styler.applymap(colorize, subset=["Rent Delta %"])
    return styler


def build_financial_comparison(
    profile: Dict[str, Any],
    crexi_df: pd.DataFrame,
    realtor_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Realtor data is currently unused but kept in the signature for backward compatibility.
    metric_defs = [
        {"label": "Asking Price", "om_key": "asking_price", "crexi_col": "asking_price", "fmt": "money0"},
        {"label": "Price/Unit", "om_key": "price_per_unit", "crexi_col": "price_per_unit", "fmt": "money0"},
        {"label": "Price/SqFt", "om_key": "price_per_sqft", "crexi_col": "price_per_sqft", "fmt": "money0"},
        {"label": "Price/Acre", "om_key": "price_per_acre", "crexi_col": "price_per_acre", "fmt": "money0"},
        {"label": "NOI", "om_key": "noi", "crexi_col": "noi", "fmt": "money0"},
        {"label": "Cap Rate", "om_key": "cap_rate", "crexi_col": "cap_rate", "fmt": "percent2"},
        {"label": "Expense Ratio", "om_key": "expense_ratio", "crexi_col": None, "fmt": "percent1"},
        {"label": "Expense Cost", "om_key": "expense_cost", "crexi_col": None, "fmt": "money0"},
    ]

    rows = []
    for metric in metric_defs:
        om_value = profile.get(metric["om_key"])

        crexi_avg = None
        crexi_count = None
        if (
            crexi_df is not None
            and not crexi_df.empty
            and metric["crexi_col"]
            and metric["crexi_col"] in crexi_df.columns
        ):
            series = crexi_df[metric["crexi_col"]].dropna()
            if not series.empty:
                crexi_avg = float(series.mean())
                crexi_count = int(series.count())

        delta_crexi = pct_diff(om_value, crexi_avg) if crexi_avg is not None else None

        rows.append(
            {
                "Metric": metric["label"],
                "OM": om_value,
                "Crexi Avg": crexi_avg,
                "Crexi Count": crexi_count,
                "OM vs Crexi %": delta_crexi,
                "format_key": metric["fmt"],
            }
        )

    df = pd.DataFrame(rows)

    display_rows = []
    for _, row in df.iterrows():
        fmt_key = row["format_key"]
        formatter = FORMATTERS.get(fmt_key, lambda v: format_number(v, 0))
        display_rows.append(
            {
                "Metric": row["Metric"],
                "OM": formatter(row["OM"]),
                "Crexi Avg": formatter(row["Crexi Avg"]) if row["Crexi Avg"] is not None else "-",
                "Crexi Count": format_number(row["Crexi Count"], 0) if row["Crexi Count"] is not None else "-",
                "OM vs Crexi %": format_percent(row["OM vs Crexi %"], 1),
            }
        )

    display_df = pd.DataFrame(display_rows)
    return df, display_df


def build_physical_summary(
    profile: Dict[str, Any],
    crexi_df: pd.DataFrame,
    realtor_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    metric_defs = [
        {"label": "Total Units", "om_value": profile.get("total_units"), "crexi_col": "units", "realtor_col": "beds", "fmt": "number0"},
        {"label": "Rentable Square Feet", "om_value": profile.get("rentable_square_footage"), "crexi_col": "sqft", "realtor_col": "sqft", "fmt": "number0"},
        {"label": "Lot Size (acres)", "om_value": profile.get("lot_size_acres"), "crexi_col": "lot_size_acres", "realtor_col": "lot_acres", "fmt": "number2"},
    ]

    rows = []
    for metric in metric_defs:
        crexi_avg = None
        crexi_count = None
        if crexi_df is not None and not crexi_df.empty and metric["crexi_col"] in crexi_df.columns:
            series = crexi_df[metric["crexi_col"]].dropna()
            if not series.empty:
                crexi_avg = float(series.mean())
                crexi_count = int(series.count())

        realtor_avg = None
        realtor_count = None
        if realtor_df is not None and not realtor_df.empty and metric["realtor_col"] in realtor_df.columns:
            series = realtor_df[metric["realtor_col"]].dropna()
            if not series.empty:
                realtor_avg = float(series.mean())
                realtor_count = int(series.count())

        rows.append(
            {
                "Metric": metric["label"],
                "OM": metric["om_value"],
                "Crexi Avg": crexi_avg,
                "Crexi Count": crexi_count,
                "Realtor Avg": realtor_avg,
                "Realtor Count": realtor_count,
                "format_key": metric["fmt"],
            }
        )

    df = pd.DataFrame(rows)

    display_rows = []
    for _, row in df.iterrows():
        fmt_key = row["format_key"]
        formatter = FORMATTERS.get(fmt_key, lambda v: format_number(v, 0))
        display_rows.append(
            {
                "Metric": row["Metric"],
                "OM": formatter(row["OM"]),
                "Crexi Avg": formatter(row["Crexi Avg"]) if row["Crexi Avg"] is not None else "-",
                "Crexi Count": format_number(row["Crexi Count"], 0) if row["Crexi Count"] is not None else "-",
                "Realtor Avg": formatter(row["Realtor Avg"]) if row["Realtor Avg"] is not None else "-",
                "Realtor Count": format_number(row["Realtor Count"], 0) if row["Realtor Count"] is not None else "-",
            }
        )

    display_df = pd.DataFrame(display_rows)
    return df, display_df


def build_comp_stats(df: pd.DataFrame, columns_map: List[Tuple[str, str, str]]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Metric", "Mean", "Median", "Std", "Min", "Max", "Count"])
    rows = []
    for column, label, fmt in columns_map:
        if column not in df.columns:
            continue
        series = df[column].dropna()
        if series.empty:
            continue
        rows.append(
            {
                "Metric": label,
                "Mean": float(series.mean()),
                "Median": float(series.median()),
                "Std": float(series.std(ddof=0)) if series.count() > 1 else 0.0,
                "Min": float(series.min()),
                "Max": float(series.max()),
                "Count": int(series.count()),
                "format_key": fmt,
            }
        )
    stats_df = pd.DataFrame(rows)
    display_rows = []
    for _, row in stats_df.iterrows():
        formatter = FORMATTERS.get(row["format_key"], lambda v: format_number(v, 2))
        display_rows.append(
            {
                "Metric": row["Metric"],
                "Mean": formatter(row["Mean"]),
                "Median": formatter(row["Median"]),
                "Std": formatter(row["Std"]),
                "Min": formatter(row["Min"]),
                "Max": formatter(row["Max"]),
                "Count": format_number(row["Count"], 0),
            }
        )
    display_df = pd.DataFrame(display_rows)
    return display_df


def compute_opportunity_zone_share(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty or "opportunity_zone_bool" not in df.columns:
        return None
    series = df["opportunity_zone_bool"].dropna()
    if series.empty:
        return None
    return float(series.mean())


def prepare_display_table(
    df: pd.DataFrame,
    columns: List[Tuple[str, str, Optional[str]]],
    limit: Optional[int] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    subset_cols = [col for col, _, _ in columns if col in df.columns]
    if not subset_cols:
        return pd.DataFrame()
    narrowed = df[subset_cols].copy()
    prepared = pd.DataFrame()
    for column, label, fmt in columns:
        if column not in narrowed.columns:
            continue
        if fmt:
            formatter = FORMATTERS.get(fmt, lambda v: format_number(v, 0))
            prepared[label] = narrowed[column].apply(formatter)
        else:
            prepared[label] = narrowed[column]
    if limit is not None:
        prepared = prepared.head(limit)
    return prepared

def run_weighted_price_model(
    profile: Dict[str, Any],
    crexi_df: Optional[pd.DataFrame],
    origin: Optional[Tuple[float, float]],
    bandwidth_miles: float,
    max_distance: float,
    selected_feature_keys: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if origin is None:
        return {"error": "Property coordinates unavailable; the spatial model needs latitude/longitude."}

    if crexi_df is None or crexi_df.empty:
        return {"error": "No CREXi comps available to fit the spatial model."}

    df = crexi_df.copy()
    if "distance_miles" not in df.columns:
        df = add_distance_column(df, origin, "latitude", "longitude")

    source_total = len(df)
    distance_mask = df["distance_miles"].notna()
    coords_total = int(distance_mask.sum())
    missing_coords = source_total - coords_total

    df = df[distance_mask]
    if df.empty:
        return {
            "error": "CREXi comps are missing coordinates; cannot compute distances.",
            "source_comps": source_total,
            "comps_with_coordinates": coords_total,
            "missing_coordinates": missing_coords,
        }

    df = df[df["distance_miles"] <= max_distance]
    if df.empty:
        return {
            "error": "No comps within the selected distance threshold.",
            "source_comps": source_total,
            "comps_with_coordinates": coords_total,
            "missing_coordinates": missing_coords,
            "within_radius": 0,
        }

    df_candidates = df.copy()
    within_radius = len(df_candidates)

    if "asking_price" not in df.columns:
        return {
            "error": "CREXi data is missing asking prices.",
            "source_comps": source_total,
            "comps_with_coordinates": coords_total,
            "missing_coordinates": missing_coords,
            "within_radius": within_radius,
        }
    df_model = df[df["asking_price"].notna()].copy()
    if df_model.empty:
        return {
            "error": "No comps with asking price data inside the distance threshold.",
            "source_comps": source_total,
            "comps_with_coordinates": coords_total,
            "missing_coordinates": missing_coords,
            "within_radius": within_radius,
        }

    feature_defs = {
        "units": {
            "label": "Units",
            "om_value": profile.get("total_units"),
            "transform": np.log1p,
            "nonnegative": True,
            "min_non_null": 5,
            "format_key": "number0",
        },
        "sqft": {
            "label": "Rentable SqFt",
            "om_value": profile.get("rentable_square_footage"),
            "transform": np.log1p,
            "nonnegative": True,
            "min_non_null": 5,
            "format_key": "number0",
        },
        "lot_size_acres": {
            "label": "Lot Size (acres)",
            "om_value": profile.get("lot_size_acres"),
            "transform": np.log1p,
            "nonnegative": True,
            "min_non_null": 5,
            "format_key": "number2",
        },
        "cap_rate": {
            "label": "Cap Rate",
            "om_value": profile.get("cap_rate"),
            "transform": lambda arr: arr,
            "nonnegative": False,
            "min_non_null": 5,
            "format_key": "percent2",
        },
    }

    if selected_feature_keys is None:
        selected_feature_keys = [
            key for key, meta in feature_defs.items() if meta["om_value"] is not None
        ]

    usable_features: List[Dict[str, Any]] = []
    for key in selected_feature_keys:
        meta = feature_defs.get(key)
        if not meta:
            continue
        om_value = meta["om_value"]
        if om_value is None or (isinstance(om_value, float) and math.isnan(om_value)):
            continue
        if key not in df_model.columns:
            continue

        series = pd.to_numeric(df_model[key], errors="coerce")
        if meta.get("nonnegative", False):
            series = series.where(series >= 0)

        df_model[key] = series
        non_null = series.notna().sum()
        if non_null < meta.get("min_non_null", 5):
            continue

        usable_features.append(
            {
                "key": key,
                "label": meta["label"],
                "transform": meta["transform"],
                "format_key": meta["format_key"],
                "om_value": float(om_value),
            }
        )

    if not usable_features:
        return {
            "error": "Selected features lack sufficient data overlap between the OM and nearby CREXi comps.",
        }

    feature_keys = [meta["key"] for meta in usable_features]
    model_mask = df_model[feature_keys + ["asking_price"]].notna().all(axis=1)
    if model_mask.sum() < len(usable_features) + 2:
        return {"error": "Too few comps with complete data to fit the spatial model."}

    df_model = df_model.loc[model_mask].copy()

    distances = df_model["distance_miles"].to_numpy(dtype=float)
    bandwidth = max(bandwidth_miles, 0.25)
    weights = np.exp(-((distances / bandwidth) ** 2))
    if not np.any(weights > 1e-6):
        return {"error": "Distance weighting eliminated all comps. Try increasing the bandwidth."}

    mask = weights > 1e-6
    df_model = df_model.loc[mask].copy()
    weights = weights[mask]
    if len(df_model) < len(usable_features) + 2:
        return {"error": "After weighting, too few comps remain to fit the model. Increase the bandwidth or radius."}

    model_indices = df_model.index.to_numpy()

    y_log = np.log1p(df_model["asking_price"].to_numpy(dtype=float))

    design_columns = [np.ones(len(df_model))]
    om_vector = [1.0]
    for meta in usable_features:
        column_data = df_model[meta["key"]].to_numpy(dtype=float)
        transform = meta["transform"]
        transformed = transform(column_data)
        design_columns.append(transformed)

        om_transformed = transform(np.array([meta["om_value"]], dtype=float))
        om_vector.append(float(om_transformed[0]))

    X = np.column_stack(design_columns)
    sqrt_w = np.sqrt(weights)
    X_weighted = X * sqrt_w[:, None]
    y_weighted = y_log * sqrt_w

    try:
        beta, _, _, _ = np.linalg.lstsq(X_weighted, y_weighted, rcond=None)
    except np.linalg.LinAlgError:
        return {"error": "Weighted regression failed to converge. Try loosening the distance/bandwidth settings."}

    prediction_log = float(np.dot(om_vector, beta))
    prediction_value = float(np.expm1(prediction_log))

    fitted_logs = X @ beta
    fitted_prices = np.expm1(fitted_logs)
    residuals = df_model["asking_price"].to_numpy(dtype=float) - fitted_prices
    weighted_rmse = float(np.sqrt(np.average(residuals ** 2, weights=weights)))
    normalized_weights = weights / weights.sum()

    weight_map = {
        idx: float(weight_pct)
        for idx, weight_pct in zip(model_indices, normalized_weights * 100.0)
    }
    price_map = {idx: float(price) for idx, price in zip(model_indices, fitted_prices)}

    chart_df = df_candidates.copy()
    chart_df["weight_pct"] = chart_df.index.map(lambda idx: weight_map.get(idx, 0.0))
    chart_df["model_price"] = chart_df.index.map(price_map.get)
    chart_df["in_model"] = chart_df.index.map(lambda idx: idx in weight_map and weight_map[idx] > 0)
    chart_df["distance_miles"] = chart_df.get("distance_miles")
    chart_df["asking_price"] = pd.to_numeric(chart_df.get("asking_price"), errors="coerce")
    chart_df["model_price"] = pd.to_numeric(chart_df["model_price"], errors="coerce")
    chart_df["residual"] = chart_df["model_price"] - chart_df["asking_price"]

    display_df = chart_df.copy()
    display_df["Weight %"] = display_df.index.map(lambda idx: weight_map.get(idx, 0.0))
    display_df["Model Price"] = display_df.index.map(price_map.get)
    display_df["In Model"] = display_df["Weight %"].apply(lambda v: "Yes" if v > 0 else "No")
    display_df["Distance (mi)"] = display_df["distance_miles"]
    display_df["Property"] = display_df.get("property_name", pd.Series(["-"] * len(display_df)))
    display_df["Address"] = display_df.get("address", pd.Series(["-"] * len(display_df)))
    if "asking_price" in display_df.columns:
        display_df["Asking Price"] = display_df["asking_price"]
    for meta in usable_features:
        display_df[meta["label"]] = pd.to_numeric(display_df.get(meta["key"]), errors="coerce")
    display_df = display_df.sort_values("Weight %", ascending=False)

    coeff_rows = [{"Feature": "Intercept", "Coefficient (log$)": beta[0]}]
    for idx, meta in enumerate(usable_features, start=1):
        coeff_rows.append(
            {
                "Feature": meta["label"],
                "Coefficient (log$)": beta[idx],
            }
        )
    coeff_df = pd.DataFrame(coeff_rows)

    om_feature_snapshot = {meta["label"]: meta["om_value"] for meta in usable_features}

    return {
        "prediction": prediction_value,
        "prediction_log": prediction_log,
        "weighted_rmse": weighted_rmse,
        "effective_comps": len(df_model),
        "candidate_comps": len(df_candidates),
        "source_comps": source_total,
        "comps_with_coordinates": coords_total,
        "missing_coordinates": missing_coords,
        "within_radius": within_radius,
        "features_used": [meta["label"] for meta in usable_features],
        "om_features": om_feature_snapshot,
        "feature_metadata": usable_features,
        "coefficients": coeff_df,
        "comp_display": display_df,
        "chart_data": chart_df,
    }


def main() -> None:
    st.set_page_config(page_title="OM Comparison Dashboard", layout="wide")
    st.title("OM Comparison Dashboard")
    st.caption("Extract OM data via Azure OpenAI and benchmark against CREXi and Realtor datasets.")

    crexi_df: Optional[pd.DataFrame] = None
    realtor_sales_df: Optional[pd.DataFrame] = None
    realtor_rent_df: Optional[pd.DataFrame] = None

    with st.sidebar:
        st.header("Data Sources")
        ok_ping, ping_msg = supabase_ping()
        st.write(f"Supabase ping: {'OK' if ok_ping else 'ERROR'}")
        st.write(ping_msg)
        st.caption(
            "Supabase tables: crexi_merged_ny_clean, realtor_properties_ny_clean, "
            "realtor_rent_clean, market_medians_all."
        )
        dataset_counts = st.session_state.get("dataset_counts")
        if dataset_counts:
            st.caption(f"CREXi rows: {dataset_counts.get('crexi', 0):,}")
            st.caption(f"Realtor sales rows: {dataset_counts.get('realtor_sales', 0):,}")
            st.caption(f"Realtor rent rows: {dataset_counts.get('realtor_rent', 0):,}")
        else:
            st.caption("Run an extraction to load market datasets from Supabase.")

        st.header("OM PDF")
        pdf_files: List[Path] = []
        if DEFAULT_PDF_FOLDER.exists():
            pdf_files = sorted(DEFAULT_PDF_FOLDER.glob("*.pdf"))
        pdf_options = [None] + pdf_files
        selected_pdf = st.selectbox(
            "Select OM from folder",
            options=pdf_options,
            format_func=lambda p: "Use uploaded file" if p is None else p.name,
        )
        uploaded_pdf = st.file_uploader("Or upload OM PDF", type=["pdf"])
        st.header("Comparison Filters")
        apply_filters = st.checkbox(
            "Apply filters",
            value=False,
            help="When disabled, comparisons use the full dataset without distance or variance limits.",
        )
        max_distance = st.slider("Max distance (miles)", min_value=1, max_value=100, value=15)
        max_units_diff = st.slider("Max unit difference", min_value=0, max_value=300, value=25)
        max_price_pct = st.slider("Max asking price difference (%)", min_value=0, max_value=100, value=15, step=5) / 100
        max_cap_pp = st.slider("Max cap rate difference (percentage points)", min_value=0.0, max_value=5.0, value=1.0, step=0.25)
        max_ppu_pct = st.slider("Max price per unit difference (%)", min_value=0, max_value=100, value=20, step=5) / 100
        max_ppsf_pct = st.slider("Max price per sqft difference (%)", min_value=0, max_value=100, value=20, step=5) / 100
        max_ppacre_pct = st.slider("Max price per acre difference (%)", min_value=0, max_value=150, value=25, step=5) / 100
        vacancy_rate = st.slider("Vacancy assumption (%)", min_value=0.0, max_value=20.0, value=VACANCY_DEFAULT * 100, step=0.5) / 100
        if not apply_filters:
            st.caption("Filters are off - full datasets are used for comparison.")

        run_button = st.button("Extract & Compare", use_container_width=True)
    max_cap_diff = max_cap_pp / 100.0

    if run_button:
        if uploaded_pdf is None and selected_pdf is None:
            st.warning("Please upload or select a PDF before running extraction.")
        else:
            pdf_path: Optional[str] = None
            temp_path: Optional[Path] = None
            source_label = "Selected PDF"
            if uploaded_pdf is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp:
                    temp.write(uploaded_pdf.getbuffer())
                    temp_path = Path(temp.name)
                pdf_path = str(temp_path)
                source_label = uploaded_pdf.name
            else:
                if selected_pdf is not None:
                    pdf_path = str(selected_pdf)
                    source_label = selected_pdf.name

            if pdf_path:
                try:
                    with st.spinner("Extracting data from OM..."):
                        images = pdf_to_images(pdf_path)
                        base64_images = images_to_base64(images)
                        extraction = call_azure_extraction(base64_images)
                    st.session_state["om_extraction"] = {
                        "data": extraction["data"],
                        "base64_images": base64_images,
                        "images": images,
                        "tokens_used": extraction.get("tokens_used", {}),
                        "source_pdf": source_label,
                    }
                    st.success("Extraction complete.")
                except Exception as exc:
                    st.error(f"Extraction failed: {exc}")
                finally:
                    if uploaded_pdf is not None and temp_path is not None:
                        temp_path.unlink(missing_ok=True)

    extraction_state = st.session_state.get("om_extraction")
    if not extraction_state:
        st.info("Upload or select an OM PDF and click **Extract & Compare** to generate the dashboard.")
        return

    om_data = extraction_state.get("data")
    if not om_data:
        st.error("No extracted data available.")
        return

    profile = build_property_profile(om_data)

    city_value = profile.get("city")
    state_value = profile.get("state")
    market_medians_df = pd.DataFrame()
    try:
        with st.spinner("Loading market datasets from Supabase..."):
            crexi_df = load_crexi_dataset(city_value, state_value)
            realtor_sales_df = load_realtor_sale_dataset(city_value, state_value)
            realtor_rent_df = load_realtor_rent_dataset(city_value, state_value)
            if city_value and state_value:
                market_medians_df = fetch_market_medians(
                    geography=f"{city_value}, {state_value}",
                    metrics=[
                        "price_per_sf_usd",
                        "price_per_unit_usd",
                        "cap_rate_pct",
                        "noi_usd",
                        "list_price_usd",
                    ],
                )
    except Exception as exc:
        st.error(
            f"Failed to load market datasets from Supabase. Check credentials and connectivity. Details: {exc}"
        )
        return

    st.session_state["dataset_counts"] = {
        "crexi": len(crexi_df) if crexi_df is not None else 0,
        "realtor_sales": len(realtor_sales_df) if realtor_sales_df is not None else 0,
        "realtor_rent": len(realtor_rent_df) if realtor_rent_df is not None else 0,
    }
    st.session_state["market_medians_df"] = market_medians_df

    filters = {
        "max_distance": float(max_distance),
        "max_units_diff": float(max_units_diff),
        "max_price_pct": max_price_pct,
        "max_cap_diff": max_cap_diff,
        "max_ppu_pct": max_ppu_pct,
        "max_ppsf_pct": max_ppsf_pct,
        "max_ppacre_pct": max_ppacre_pct,
    }

    origin = resolve_property_coordinates(profile, [crexi_df, realtor_sales_df, realtor_rent_df])

    crexi_distance_only = crexi_df.copy() if crexi_df is not None else pd.DataFrame()
    if not crexi_distance_only.empty:
        crexi_distance_only = add_distance_column(crexi_distance_only, origin, "latitude", "longitude")

    if apply_filters:
        crexi_filtered = apply_crexi_filters(crexi_df, profile, filters, origin)
        realtor_sales_filtered = filter_realtor_sales(realtor_sales_df, profile, filters, origin)
        realtor_rents_filtered = filter_realtor_rents(realtor_rent_df, profile, filters, origin)
    else:
        crexi_filtered = crexi_df.copy() if crexi_df is not None else pd.DataFrame()
        if not crexi_filtered.empty:
            crexi_filtered = add_distance_column(crexi_filtered, origin, "latitude", "longitude")

        realtor_sales_filtered = realtor_sales_df.copy() if realtor_sales_df is not None else pd.DataFrame()
        if not realtor_sales_filtered.empty:
            realtor_sales_filtered = add_distance_column(realtor_sales_filtered, origin, "latitude", "longitude")

        realtor_rents_filtered = realtor_rent_df.copy() if realtor_rent_df is not None else pd.DataFrame()
        if not realtor_rents_filtered.empty:
            realtor_rents_filtered = add_distance_column(realtor_rents_filtered, origin, "latitude", "longitude")
    rent_table, rent_totals = build_rent_comparison(profile.get("unit_info", {}), realtor_rents_filtered, vacancy_rate)
    financial_raw, financial_display = build_financial_comparison(profile, crexi_filtered, realtor_sales_filtered)
    physical_raw, physical_display = build_physical_summary(profile, crexi_filtered, realtor_sales_filtered)

    crexi_stats = build_comp_stats(
        crexi_filtered,
        [
            ("asking_price", "Asking Price", "money0"),
            ("price_per_unit", "Price per Unit", "money0"),
            ("price_per_sqft", "Price per SqFt", "money0"),
            ("cap_rate", "Cap Rate", "percent2"),
            ("noi", "NOI", "money0"),
            ("units", "Units", "number0"),
            ("sqft", "Rentable SqFt", "number0"),
            ("lot_size_acres", "Lot Size (acres)", "number2"),
            ("distance_miles", "Distance (mi)", "number1"),
        ],
    )

    realtor_sale_stats = build_comp_stats(
        realtor_sales_filtered,
        [
            ("list_price", "List Price", "money0"),
            ("price_per_sqft", "Price/SqFt", "money0"),
            ("lot_acres", "Lot Acres", "number2"),
            ("beds", "Beds", "number0"),
            ("sqft", "SqFt", "number0"),
            ("distance_miles", "Distance (mi)", "number1"),
        ],
    )

    rent_stats = build_comp_stats(
        realtor_rents_filtered,
        [
            ("asking_rent", "Asking Rent", "money0"),
            ("beds", "Beds", "number0"),
            ("sqft", "SqFt", "number0"),
            ("distance_miles", "Distance (mi)", "number1"),
        ],
    )

    oz_share = compute_opportunity_zone_share(crexi_filtered)

    if ACTIVE_TAB_KEY not in st.session_state:
        st.session_state[ACTIVE_TAB_KEY] = TAB_LABELS[0]

    overview_tab, rent_tab, financial_tab, spatial_tab, comps_tab, raw_tab, pdf_tab = st.tabs(
        TAB_LABELS,
        default=st.session_state.get(ACTIVE_TAB_KEY, TAB_LABELS[0]),
    )

    with overview_tab:
        st.subheader("Property Summary")
        if profile.get("address"):
            st.markdown(f"**Address:** {profile['address']}")
        if profile.get("summary"):
            st.info(profile["summary"])

        overview_metrics = [
            ("Total Units", profile.get("total_units"), lambda v: format_number(v, 0)),
            ("Rentable SF", profile.get("rentable_square_footage"), lambda v: format_number(v, 0)),
            ("Lot Size (acres)", profile.get("lot_size_acres"), lambda v: format_number(v, 2)),
            ("Property Age", profile.get("property_age"), lambda v: format_number(v, 0)),
            ("Year Renovated", profile.get("year_renovated"), lambda v: format_number(v, 0)),
            ("OZ Status", profile.get("oz_status"), lambda v: "Yes" if v else "No" if v is not None else "-"),
            ("Asking Price", profile.get("asking_price"), format_money),
            ("NOI", profile.get("noi"), format_money),
            ("Cap Rate", profile.get("cap_rate"), lambda v: format_percent(v, 2)),
            ("Expense Ratio", profile.get("expense_ratio"), lambda v: format_percent(v, 1)),
            ("Expense Cost", profile.get("expense_cost"), format_money),
            ("Price / Unit", profile.get("price_per_unit"), format_money),
            ("Price / SqFt", profile.get("price_per_sqft"), format_money),
            ("Price / Acre", profile.get("price_per_acre"), format_money),
        ]

        cols = st.columns(3)
        for idx, (label, value, formatter) in enumerate(overview_metrics):
            display_value = formatter(value) if callable(formatter) else value
            cols[idx % 3].metric(label, display_value)

        if origin is None:
            st.warning("Could not resolve property coordinates; distance filters use city/state matching only.")

        count_cols = st.columns(3)
        count_cols[0].metric("CREXi comps matched", f"{len(crexi_filtered):,}")
        count_cols[1].metric("Realtor sale comps matched", f"{len(realtor_sales_filtered):,}")
        count_cols[2].metric("Realtor rent comps matched", f"{len(realtor_rents_filtered):,}")

        if oz_share is not None:
            st.caption(f"CREXi Opportunity Zone share (filtered): {format_percent(oz_share, 1)}")

        tokens = extraction_state.get("tokens_used", {})
        if tokens:
            prompt_tokens = tokens.get("prompt_tokens")
            completion_tokens = tokens.get("completion_tokens")
            total_tokens = tokens.get("total_tokens")
            st.caption(
                f"Tokens - prompt: {prompt_tokens}, completion: {completion_tokens}, total: {total_tokens}"
            )

        json_payload = json.dumps(om_data, indent=2)
        st.download_button(
            "Download extracted JSON",
            data=json_payload,
            file_name="om_extracted.json",
            mime="application/json",
        )

    with rent_tab:
        st.subheader("Unit Rent Comparison")
        if rent_table.empty:
            st.info("No Realtor rent comps matched the current filters.")
        else:
            summary_cols = st.columns(4)
            delta_label = (
                f"{rent_totals['rent_delta_total'] * 100:.1f}% vs market"
                if rent_totals.get("rent_delta_total") is not None
                else None
            )
            summary_cols[0].metric("Weighted OM Rent", format_money(rent_totals.get("weighted_om_avg")), delta=delta_label)
            summary_cols[1].metric("Weighted Market Rent", format_money(rent_totals.get("weighted_market_avg")))
            summary_cols[2].metric("OM GPR (annual)", format_money(rent_totals.get("gpr_om"), 0))
            summary_cols[3].metric("Market GPR (annual)", format_money(rent_totals.get("gpr_market"), 0))

            st.caption("Effective gross income applies the selected vacancy rate to annual GPR.")
            egi_rows = [
                {"Metric": "OM EGI", "Value": format_money(rent_totals.get("egi_om"), 0)},
                {"Metric": "Market EGI", "Value": format_money(rent_totals.get("egi_market"), 0)},
                {"Metric": "Vacancy Rate", "Value": format_percent(rent_totals.get("vacancy_rate"), 1)},
            ]
            egi_df = pd.DataFrame(egi_rows)
            st.dataframe(egi_df, use_container_width=True, hide_index=True)

            st.dataframe(style_rent_table(rent_table), use_container_width=True, hide_index=True)

    with financial_tab:
        st.subheader("Financial Metrics")
        if financial_display.empty:
            st.info("No financial comparison available with current filters.")
        else:
            st.dataframe(financial_display, use_container_width=True, hide_index=True)

        st.subheader("Physical Snapshot")
        if physical_display.empty:
            st.info("No physical comparison available with current filters.")
        else:
            st.dataframe(physical_display, use_container_width=True, hide_index=True)

    with spatial_tab:
        st.subheader("Spatial Pricing Model")
        if crexi_distance_only.empty:
            st.info("No nearby CREXi comps for the regression; GWR map below will still render using fallbacks.")
        else:
            remember_spatial_tab = partial(remember_tab, "Spatial Pricing")
            controls = st.columns(2)
            default_radius = max(5, min(int(max_distance), 50))
            spatial_radius = controls[0].slider(
                "Model radius (miles)",
                min_value=5,
                max_value=100,
                value=default_radius,
                step=5,
                key="spatial_radius_miles",
                on_change=remember_spatial_tab,
            )
            default_bandwidth = max(1.0, min(float(max_distance), 10.0))
            spatial_bandwidth = controls[1].slider(
                "Distance bandwidth (miles)",
                min_value=0.5,
                max_value=50.0,
                value=default_bandwidth,
                step=0.5,
                key="spatial_bandwidth_miles",
                on_change=remember_spatial_tab,
            )
            if spatial_bandwidth > spatial_radius:
                spatial_bandwidth = float(spatial_radius)

            feature_candidates = [
                {"key": "units", "label": "Units", "om_value": profile.get("total_units")},
                {"key": "sqft", "label": "Rentable SqFt", "om_value": profile.get("rentable_square_footage")},
                {"key": "lot_size_acres", "label": "Lot Size (acres)", "om_value": profile.get("lot_size_acres")},
                {"key": "cap_rate", "label": "Cap Rate", "om_value": profile.get("cap_rate")},
            ]
            available_features = [item for item in feature_candidates if item["om_value"] is not None]
            if not available_features:
                st.warning("No OM features available to fit the spatial model.")
            else:
                default_labels = [item["label"] for item in available_features]
                selected_labels = st.multiselect(
                    "Model features",
                    options=[item["label"] for item in available_features],
                    default=default_labels,
                    help="Choose which OM metrics the spatial regression should match.",
                    key="spatial_feature_labels",
                    on_change=remember_spatial_tab,
                )
                selected_keys = [
                    item["key"] for item in available_features if item["label"] in selected_labels
                ]

                if not selected_keys:
                    st.info("Select at least one feature to run the spatial model.")
                else:
                    model_result = run_weighted_price_model(
                        profile,
                        crexi_distance_only,
                        origin,
                        bandwidth_miles=float(spatial_bandwidth),
                        max_distance=float(spatial_radius),
                        selected_feature_keys=selected_keys,
                    )

                    if "error" in model_result:
                        st.warning(model_result["error"])
                        detail_parts: List[str] = []
                        source_total = model_result.get("source_comps")
                        coords_total = model_result.get("comps_with_coordinates")
                        missing_coords = model_result.get("missing_coordinates")
                        within_radius = model_result.get("within_radius")
                        if source_total is not None:
                            detail_parts.append(f"CREXi comps: {int(source_total):,}")
                        if coords_total is not None:
                            detail_parts.append(f"With coordinates: {int(coords_total):,}")
                        if missing_coords:
                            detail_parts.append(f"Missing coordinates: {int(missing_coords):,}")
                        if within_radius is not None:
                            detail_parts.append(f"Within radius: {int(within_radius):,}")
                        if detail_parts:
                            st.caption(" | ".join(detail_parts))
                    else:
                        predicted_price = model_result["prediction"]
                        om_price = profile.get("asking_price")
                        delta_value = None
                        if om_price is not None and not (
                            isinstance(om_price, float) and math.isnan(om_price)
                        ):
                            delta_value = predicted_price - om_price
                        metric_delta = format_money(delta_value, 0) if delta_value is not None else None
                        st.metric(
                            "Model Asking Price",
                            format_money(predicted_price, 0),
                            delta=metric_delta,
                        )

                        weighted_rmse = model_result.get("weighted_rmse")
                        rmse_display = (
                            format_money(weighted_rmse, 0) if weighted_rmse is not None else "-"
                        )
                        candidate_total = model_result.get("candidate_comps")
                        source_total = model_result.get("source_comps")
                        coords_total = model_result.get("comps_with_coordinates")
                        missing_coords = model_result.get("missing_coordinates")
                        caption_main: str
                        if candidate_total is not None:
                            caption_main = (
                                f"Comps used in regression: {model_result['effective_comps']:,} of "
                                f"{candidate_total:,} within radius"
                            )
                        else:
                            caption_main = f"Comps used in regression: {model_result['effective_comps']:,}"
                        coord_notes: List[str] = []
                        if coords_total is not None:
                            coord_notes.append(f"{int(coords_total):,} with coordinates")
                        if missing_coords:
                            coord_notes.append(f"{int(missing_coords):,} missing coordinates")
                        if source_total is not None:
                            coord_notes.append(f"{int(source_total):,} total CREXi comps")
                        if coord_notes:
                            caption_main += " (" + ", ".join(coord_notes) + ")"
                        caption_text = f"{caption_main} - Weighted RMSE: {rmse_display}"
                        st.caption(caption_text)
                        if model_result.get("features_used"):
                            st.caption(
                                "Features in model: " + ", ".join(model_result["features_used"])
                            )

                        chart_df = model_result.get("chart_data")
                        if isinstance(chart_df, pd.DataFrame) and not chart_df.empty:
                            chart_numeric = chart_df.copy()
                            chart_numeric = chart_numeric[
                                chart_numeric["model_price"].notna() & chart_numeric["asking_price"].notna()
                            ].copy()
                            if not chart_numeric.empty:
                                chart_numeric["weight_pct"] = chart_numeric["weight_pct"].fillna(0.0)
                                chart_numeric["distance_miles"] = chart_numeric["distance_miles"].fillna(0.0)
                                chart_numeric["residual"] = chart_numeric["residual"].fillna(0.0)
                                if "property_name" not in chart_numeric.columns:
                                    chart_numeric["property_name"] = ""
                                chart_numeric["property_name"] = chart_numeric["property_name"].astype(str)
                                if "address" in chart_numeric.columns:
                                    chart_numeric["address"] = chart_numeric["address"].astype(str)
                                else:
                                    chart_numeric["address"] = ""

                                scatter = (
                                    alt.Chart(chart_numeric)
                                    .mark_circle(opacity=0.7)
                                    .encode(
                                        x=alt.X("asking_price:Q", title="Actual Asking Price"),
                                        y=alt.Y("model_price:Q", title="Model Price"),
                                        size=alt.Size(
                                            "weight_pct:Q",
                                            title="Weight %",
                                            scale=alt.Scale(range=[20, 400]),
                                        ),
                                        color=alt.Color(
                                            "distance_miles:Q",
                                            title="Distance (mi)",
                                            scale=alt.Scale(scheme="viridis"),
                                        ),
                                        tooltip=[
                                            alt.Tooltip("property_name:N", title="Property"),
                                            alt.Tooltip("address:N", title="Address"),
                                            alt.Tooltip("asking_price:Q", title="Actual Price", format=",.0f"),
                                            alt.Tooltip("model_price:Q", title="Model Price", format=",.0f"),
                                            alt.Tooltip("weight_pct:Q", title="Weight %", format=".1f"),
                                            alt.Tooltip("distance_miles:Q", title="Distance (mi)", format=".2f"),
                                            alt.Tooltip("residual:Q", title="Residual", format=",.0f"),
                                        ],
                                    )
                                )
                                value_min = float(
                                    min(chart_numeric["asking_price"].min(), chart_numeric["model_price"].min())
                                )
                                value_max = float(
                                    max(chart_numeric["asking_price"].max(), chart_numeric["model_price"].max())
                                )
                                identity = (
                                    alt.Chart(
                                        pd.DataFrame(
                                            {"price": [value_min, value_max]}
                                        )
                                    )
                                    .mark_line(color="#d62728", strokeDash=[4, 4])
                                    .encode(x="price:Q", y="price:Q")
                                )
                                st.markdown("**Actual vs model price**")
                                st.altair_chart(
                                    (scatter + identity).properties(height=360).interactive(),
                                    use_container_width=True,
                                )

                                residual_chart = (
                                    alt.Chart(chart_numeric)
                                    .mark_bar()
                                    .encode(
                                        y=alt.Y(
                                            "property_name:N",
                                            title="Property",
                                            sort="-x",
                                        ),
                                        x=alt.X(
                                            "residual:Q",
                                            title="Residual (Model - Actual)",
                                            axis=alt.Axis(format=",.0f"),
                                        ),
                                        color=alt.condition(
                                            alt.datum.residual >= 0,
                                            alt.value("#1b9e77"),
                                            alt.value("#d95f02"),
                                        ),
                                        tooltip=[
                                            alt.Tooltip("property_name:N", title="Property"),
                                            alt.Tooltip("residual:Q", title="Residual", format=",.0f"),
                                            alt.Tooltip("weight_pct:Q", title="Weight %", format=".1f"),
                                        ],
                                    )
                                )
                                st.markdown("**Residuals (positive = model higher than actual)**")
                                st.altair_chart(
                                    residual_chart.properties(height=360),
                                    use_container_width=True,
                                )

                        feature_metadata = model_result.get("feature_metadata", [])
                        feature_rows = []
                        for meta in feature_metadata:
                            formatter = FORMATTERS.get(meta["format_key"], lambda v: format_number(v, 2))
                            formatted_value = formatter(meta["om_value"])
                            feature_rows.append(
                                {
                                    "Feature": meta["label"],
                                    "OM Value": formatted_value,
                                }
                            )
                        if feature_rows:
                            st.markdown("**Model inputs**")
                            st.dataframe(
                                pd.DataFrame(feature_rows),
                                use_container_width=True,
                                hide_index=True,
                            )

                        comp_display = model_result["comp_display"].copy()
                        if not comp_display.empty:
                            if "Distance (mi)" in comp_display.columns:
                                comp_display["Distance (mi)"] = comp_display["Distance (mi)"].apply(
                                    lambda v: format_number(v, 1) if pd.notna(v) else "-"
                                )
                            if "Weight %" in comp_display.columns:
                                comp_display["Weight %"] = comp_display["Weight %"].apply(
                                    lambda v: f"{v:.1f}%"
                                )
                            asking_series = comp_display.get("Asking Price")
                            if asking_series is not None:
                                comp_display["Asking Price"] = asking_series.apply(format_money)
                            model_series = comp_display.get("Model Price")
                            if model_series is not None:
                                comp_display["Model Price"] = model_series.apply(
                                    lambda v: format_money(v) if pd.notna(v) else "-"
                                )
                            for meta in feature_metadata:
                                column = meta["label"]
                                if column in comp_display.columns:
                                    formatter = FORMATTERS.get(
                                        meta["format_key"], lambda v: format_number(v, 2)
                                    )
                                    comp_display[column] = comp_display[column].apply(
                                        lambda v: formatter(v) if pd.notna(v) else "-"
                                    )
                            display_columns = [
                                col
                                for col in [
                                    "Property",
                                    "Address",
                                    "Distance (mi)",
                                    "Weight %",
                                    "In Model",
                                    "Asking Price",
                                    "Model Price",
                                ]
                                if col in comp_display.columns
                            ]
                            for meta in feature_metadata:
                                if meta["label"] in comp_display.columns:
                                    display_columns.append(meta["label"])
                            comp_display = comp_display[display_columns]
                            st.markdown("**Comps within radius**")
                            st.caption(
                                "`In Model` = Yes denotes comps used in the weighted regression. Weight 0% means the comp was outside the modeled feature set but still falls within the distance filter."
                            )
                            st.dataframe(comp_display, use_container_width=True, hide_index=True)

                        coeff_df = model_result["coefficients"].copy()
                        if not coeff_df.empty:
                            coeff_df["Coefficient (log$)"] = coeff_df["Coefficient (log$)"].apply(
                                lambda v: f"{v:.4f}"
                            )
                            st.markdown("**Model coefficients (log-dollar space)**")
                            st.dataframe(coeff_df, use_container_width=True, hide_index=True)

        st.divider()
        render_gwr_map_inline(st.session_state.get("om_city", "Brooklyn"), st.session_state.get("om_state", "NY"))

    with comps_tab:
        st.subheader("CREXi Comp Stats")
        if crexi_filtered.empty:
            st.info("No CREXi comps matched the filters.")
        else:
            st.dataframe(crexi_stats, use_container_width=True, hide_index=True)
            crexi_display = prepare_display_table(
                crexi_filtered,
                [
                    ("property_name", "Property", None),
                    ("address", "Address", None),
                    ("city", "City", None),
                    ("state", "State", None),
                    ("units", "Units", "number0"),
                    ("asking_price", "Asking Price", "money0"),
                    ("cap_rate", "Cap Rate", "percent2"),
                    ("price_per_unit", "Price/Unit", "money0"),
                    ("price_per_sqft", "Price/SqFt", "money0"),
                    ("distance_miles", "Distance (mi)", "number1"),
                ],
                limit=50,
            )
            st.markdown("**Sample of filtered CREXi comps (first 50)**")
            st.dataframe(crexi_display, use_container_width=True, hide_index=True)
            crexi_csv = crexi_filtered.to_csv(index=False)
            st.download_button(
                "Download CREXi comps (CSV)",
                data=crexi_csv,
                file_name="crexi_filtered.csv",
                mime="text/csv",
            )

        st.subheader("Realtor Sale Comp Stats")
        if realtor_sales_filtered.empty:
            st.info("No Realtor sale comps matched the filters.")
        else:
            st.dataframe(realtor_sale_stats, use_container_width=True, hide_index=True)
            realtor_sale_display = prepare_display_table(
                realtor_sales_filtered,
                [
                    ("address", "Address", None),
                    ("city", "City", None),
                    ("state", "State", None),
                    ("list_price", "List Price", "money0"),
                    ("price_per_sqft", "Price/SqFt", "money0"),
                    ("lot_acres", "Lot Acres", "number2"),
                    ("beds", "Beds", "number0"),
                    ("sqft", "SqFt", "number0"),
                    ("distance_miles", "Distance (mi)", "number1"),
                ],
                limit=50,
            )
            st.markdown("**Sample of filtered Realtor sale comps (first 50)**")
            st.dataframe(realtor_sale_display, use_container_width=True, hide_index=True)
            realtor_sale_csv = realtor_sales_filtered.to_csv(index=False)
            st.download_button(
                "Download Realtor sale comps (CSV)",
                data=realtor_sale_csv,
                file_name="realtor_sales_filtered.csv",
                mime="text/csv",
            )

        st.subheader("Realtor Rent Comp Stats")
        if realtor_rents_filtered.empty:
            st.info("No Realtor rent comps matched the filters.")
        else:
            st.dataframe(rent_stats, use_container_width=True, hide_index=True)
            rent_display = prepare_display_table(
                realtor_rents_filtered,
                [
                    ("address", "Address", None),
                    ("city", "City", None),
                    ("state", "State", None),
                    ("beds", "Beds", "number0"),
                    ("asking_rent", "Asking Rent", "money0"),
                    ("sqft", "SqFt", "number0"),
                    ("distance_miles", "Distance (mi)", "number1"),
                ],
                limit=50,
            )
            st.markdown("**Sample of filtered Realtor rent comps (first 50)**")
            st.dataframe(rent_display, use_container_width=True, hide_index=True)
            rent_csv = realtor_rents_filtered.to_csv(index=False)
            st.download_button(
                "Download Realtor rent comps (CSV)",
                data=rent_csv,
                file_name="realtor_rents_filtered.csv",
                mime="text/csv",
            )

    with raw_tab:
        st.subheader("Raw Extracted JSON")
        st.json(om_data)

    with pdf_tab:
        st.subheader("PDF Preview")
        images: List[Image.Image] = extraction_state.get("images") or []
        if not images:
            st.info("PDF preview unavailable.")
        else:
            total_pages = len(images)
            if total_pages <= 1:
                page = 1
            else:
                page = st.slider(
                    "Page",
                    min_value=1,
                    max_value=total_pages,
                    value=1,
                    key="pdf_preview_page",
                    on_change=partial(remember_tab, "PDF Preview"),
                )
            st.image(images[page - 1], use_column_width=True)
            st.caption(f"Showing page {page} of {total_pages}")
 
def render_gwr_map_inline(city, state):
    import streamlit as st  # noqa: F811
    import pandas as pd  # noqa: F811
    import pydeck as pdk

    ok, msg = supabase_ping()
    if not ok:
        st.error(f"Supabase not reachable: {msg}")
        return

    st.subheader("GWR Map (Beta)")
    provider = st.selectbox("Provider", ["CREXi (price/sf)", "Realtor (price/sf)"], key="gwr_provider_inline")
    bandwidth = st.slider("Bandwidth (miles)", 2.0, 25.0, 8.0, 0.5, key="gwr_bw_inline")
    grid_step = st.slider("Grid step (miles)", 0.5, 10.0, 2.0, 0.5, key="gwr_step_inline")

    try:
        if provider.startswith("CREXi"):
            df, used_geo = fetch_crexi_with_fallback(city=city, state=state, limit=5000)
            lat_col, lon_col, metric_col = "Latitude", "Longitude", "Price/SqFt"
        else:
            df, used_geo = fetch_realtor_props_with_fallback(city=city, state=state, limit=5000)
            lat_col, lon_col = "latitude", "longitude"
            df["_ppsf"] = pd.to_numeric(df.get("price_per_sqft"), errors="coerce")
            if df["_ppsf"].isna().all():
                lp = pd.to_numeric(df.get("list_price"), errors="coerce")
                sf = pd.to_numeric(df.get("sqft"), errors="coerce")
                df["_ppsf"] = lp / sf.replace(0, pd.NA)
            metric_col = "_ppsf"

        df[metric_col] = pd.to_numeric(df[metric_col], errors="coerce")
        df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
        df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
        df = df.dropna(subset=[metric_col, lat_col, lon_col])
        st.info(f"Using geography: {used_geo} | Samples: {len(df)}")
    except Exception as exc:
        st.error(f"Failed to load data for GWR map: {exc}")
        return

    st.write(f"Samples: {len(df)}")
    if df.empty:
        st.info("No data available for GWR map with current filters.")
        return

    try:
        surf = run_teammate_gwr(
            df,
            metric_col=metric_col,
            lat_col=lat_col,
            lon_col=lon_col,
            bandwidth_mi=bandwidth,
            grid_step_mi=grid_step,
        )
        if surf.empty:
            surf = gwr_surface(
                df,
                metric_col=metric_col,
                lat_col=lat_col,
                lon_col=lon_col,
                bandwidth_mi=bandwidth,
                grid_step_mi=grid_step,
            )
    except Exception as exc:
        st.error(f"Failed to compute GWR surface: {exc}")
        return

    surf["pred"] = pd.to_numeric(surf.get("pred"), errors="coerce")
    surf = surf.dropna(subset=["lat", "lon", "pred"])
    if surf.empty:
        st.warning("Not enough data to compute surface.")
        return

    mid_lat = float(surf["lat"].mean())
    mid_lon = float(surf["lon"].mean())
    pmin, pmax = surf["pred"].min(), surf["pred"].max()
    scale = (pmax - pmin) if pmax != pmin else 1e-6
    surf["norm"] = (surf["pred"] - pmin) / scale
    palette = [
        [0, 0, 255],
        [0, 255, 255],
        [0, 255, 0],
        [255, 255, 0],
        [255, 165, 0],
        [255, 0, 0],
    ]
    surf["color_idx"] = (surf["norm"] * 5).clip(0, 5).round().astype(int)
    surf["color"] = surf["color_idx"].map({i: palette[i] for i in range(6)})
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=surf.dropna(subset=["lat", "lon"]),
        get_position="[lon, lat]",
        get_radius=120,
        get_fill_color="color",
        pickable=True,
    )
    tooltip = {
        "html": "<b>Predicted $/sf:</b> {pred}<br/><b>Samples used:</b> {n_used}",
        "style": {"backgroundColor": "steelblue", "color": "white"},
    }
    view_state = pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=10)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))
    st.caption("Colors represent predicted price/sf. Beta implementation; values are indicative only.")


if __name__ == "__main__":
    main()
