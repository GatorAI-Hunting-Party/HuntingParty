from __future__ import annotations

import json
import math
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pgeocode
import streamlit as st
from PIL import Image

import sys
from pathlib import Path
# Add the OM_Scraper directory to Python path
# Use absolute path resolution to avoid issues with Streamlit's working directory
current_file = Path(__file__).resolve()
frontend_dir = current_file.parent
project_root = frontend_dir.parent
om_scraper_path = project_root / "OM_Scraper"
sys.path.insert(0, str(om_scraper_path))

from om_extractor import (
    DEFAULT_MAX_PAGES,
    call_azure_extraction,
    images_to_base64,
    pdf_to_images,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PDF_FOLDER = (REPO_ROOT / "OMs").resolve()
DEFAULT_CREXI_PATH = REPO_ROOT / "backend" / "data" / "crexi_merged_ny" / "merged_properties.csv"
DEFAULT_REALTOR_SALE_PATH = REPO_ROOT / "backend" / "data" / "realtor_merged" / "properties_New_York_20251017_225557.csv"
DEFAULT_REALTOR_RENT_PATH = REPO_ROOT / "backend" / "data" / "realtor_merged" / "realtor_rent.csv"

EARTH_RADIUS_MILES = 3958.7613
VACANCY_DEFAULT = 0.06

GEOCODER = pgeocode.Nominatim("us")

MISSING_STRINGS = {"", "na", "n/a", "none", "null", "-", "--"}

BEDROOM_MAP = [
    ("1 Bed", "1_bed", 1),
    ("2 Bed", "2_bed", 2),
    ("3 Bed", "3_bed", 3),
    ("4 Bed", "4_bed", 4),
]


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
    try:
        result = GEOCODER.query_postal_code(zip_code)
    except Exception:
        return None
    if result is None:
        return None
    lat = result.latitude
    lon = result.longitude
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
def load_crexi_dataset(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    rename_map = {
        "Property Link": "property_link",
        "Property Name": "property_name",
        "Property Status": "property_status",
        "Type": "property_type",
        "Address": "address",
        "City": "city",
        "State": "state",
        "Zip": "zip_code",
        "SqFt": "sqft",
        "Lot Size": "lot_size_acres",
        "Units": "units",
        "Price/Unit": "price_per_unit",
        "NOI": "noi",
        "Cap Rate": "cap_rate",
        "Asking Price": "asking_price",
        "Price/SqFt": "price_per_sqft",
        "Price/Acre": "price_per_acre",
        "Opportunity Zone": "opportunity_zone",
        "Longitude": "longitude",
        "Latitude": "latitude",
    }
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
        df[col] = pd.to_numeric(df.get(col), errors="coerce")

    df["cap_rate"] = df["cap_rate"].apply(lambda x: x / 100.0 if pd.notna(x) and x > 1 else x)
    mask = df["price_per_unit"].isna() & df["asking_price"].notna() & df["units"].notna() & (df["units"] != 0)
    df.loc[mask, "price_per_unit"] = df.loc[mask, "asking_price"] / df.loc[mask, "units"]

    mask = df["price_per_sqft"].isna() & df["asking_price"].notna() & df["sqft"].notna() & (df["sqft"] != 0)
    df.loc[mask, "price_per_sqft"] = df.loc[mask, "asking_price"] / df.loc[mask, "sqft"]

    mask = df["price_per_acre"].isna() & df["asking_price"].notna() & df["lot_size_acres"].notna() & (df["lot_size_acres"] != 0)
    df.loc[mask, "price_per_acre"] = df.loc[mask, "asking_price"] / df.loc[mask, "lot_size_acres"]

    df["opportunity_zone_bool"] = df["opportunity_zone"].apply(parse_bool)

    df["city"] = df["city"].astype(str).str.strip()
    df["city_upper"] = df["city"].str.upper()
    df["state"] = df["state"].astype(str).str.upper().str.strip()
    df["zip_code"] = df["zip_code"].astype(str).str.strip()

    return df

@st.cache_data(show_spinner=False)
def load_realtor_sale_dataset(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
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

    rename_map = {
        "formatted_address": "address",
    }
    df = df.rename(columns=rename_map)

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

    df["price_per_acre"] = np.where(
        (df.get("list_price").notna()) & (df["lot_acres"].notna()) & (df["lot_acres"] != 0),
        df["list_price"] / df["lot_acres"],
        np.nan,
    )

    df["city"] = df.get("city", pd.Series(dtype=str)).astype(str).str.strip()
    df["city_upper"] = df["city"].str.upper()
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.upper().str.strip()
    if "zip_code" in df.columns:
        df["zip_code"] = df["zip_code"].astype(str).str.strip()

    return df


@st.cache_data(show_spinner=False)
def load_realtor_rent_dataset(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
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
    metric_defs = [
        {"label": "Asking Price", "om_key": "asking_price", "crexi_col": "asking_price", "realtor_col": "list_price", "fmt": "money0"},
        {"label": "Price/Unit", "om_key": "price_per_unit", "crexi_col": "price_per_unit", "realtor_col": None, "fmt": "money0"},
        {"label": "Price/SqFt", "om_key": "price_per_sqft", "crexi_col": "price_per_sqft", "realtor_col": "price_per_sqft", "fmt": "money0"},
        {"label": "Price/Acre", "om_key": "price_per_acre", "crexi_col": "price_per_acre", "realtor_col": "price_per_acre", "fmt": "money0"},
        {"label": "NOI", "om_key": "noi", "crexi_col": "noi", "realtor_col": None, "fmt": "money0"},
        {"label": "Cap Rate", "om_key": "cap_rate", "crexi_col": "cap_rate", "realtor_col": None, "fmt": "percent2"},
        {"label": "Expense Ratio", "om_key": "expense_ratio", "crexi_col": None, "realtor_col": None, "fmt": "percent1"},
        {"label": "Expense Cost", "om_key": "expense_cost", "crexi_col": None, "realtor_col": None, "fmt": "money0"},
    ]

    rows = []
    for metric in metric_defs:
        om_value = profile.get(metric["om_key"])

        crexi_avg = None
        crexi_count = None
        if crexi_df is not None and not crexi_df.empty and metric["crexi_col"] and metric["crexi_col"] in crexi_df.columns:
            series = crexi_df[metric["crexi_col"]].dropna()
            if not series.empty:
                crexi_avg = float(series.mean())
                crexi_count = int(series.count())

        realtor_avg = None
        realtor_count = None
        if realtor_df is not None and not realtor_df.empty and metric["realtor_col"] and metric["realtor_col"] in realtor_df.columns:
            series = realtor_df[metric["realtor_col"]].dropna()
            if not series.empty:
                realtor_avg = float(series.mean())
                realtor_count = int(series.count())

        delta_crexi = pct_diff(om_value, crexi_avg) if crexi_avg is not None else None
        delta_realtor = pct_diff(om_value, realtor_avg) if realtor_avg is not None else None

        rows.append(
            {
                "Metric": metric["label"],
                "OM": om_value,
                "Crexi Avg": crexi_avg,
                "Crexi Count": crexi_count,
                "Realtor Avg": realtor_avg,
                "Realtor Count": realtor_count,
                "OM vs Crexi %": delta_crexi,
                "OM vs Realtor %": delta_realtor,
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
                "OM vs Crexi %": format_percent(row["OM vs Crexi %"], 1),
                "OM vs Realtor %": format_percent(row["OM vs Realtor %"], 1),
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

def main() -> None:
    st.set_page_config(page_title="OM Comparison Dashboard", layout="wide")
    st.title("OM Comparison Dashboard")
    st.caption("Extract OM data via Azure OpenAI and benchmark against CREXi and Realtor datasets.")

    crexi_df: Optional[pd.DataFrame] = None
    realtor_sales_df: Optional[pd.DataFrame] = None
    realtor_rent_df: Optional[pd.DataFrame] = None

    with st.sidebar:
        st.header("Data Sources")
        crexi_path = DEFAULT_CREXI_PATH
        realtor_sale_path = DEFAULT_REALTOR_SALE_PATH
        realtor_rent_path = DEFAULT_REALTOR_RENT_PATH

        # st.caption(f"CREXi source: {crexi_path}")
        # st.caption(f"Realtor sales source: {realtor_sale_path}")
        # st.caption(f"Realtor rent source: {realtor_rent_path}")

        def _load_dataset(path: Path, loader, label: str) -> Optional[pd.DataFrame]:
            if not path.exists():
                st.error(f"{label} file not found: {path}")
                return None
            try:
                df = loader(str(path))
                st.caption(f"{label} rows: {len(df):,}")
                return df
            except Exception as exc:
                st.error(f"{label} load failed: {exc}")
                return None

        crexi_df = _load_dataset(crexi_path, load_crexi_dataset, "CREXi")
        realtor_sales_df = _load_dataset(realtor_sale_path, load_realtor_sale_dataset, "Realtor sales")
        realtor_rent_df = _load_dataset(realtor_rent_path, load_realtor_rent_dataset, "Realtor rent")

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
        max_pages = st.slider("Pages to send to model", min_value=1, max_value=20, value=min(DEFAULT_MAX_PAGES, 12))

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
                        images = pdf_to_images(pdf_path, max_pages=max_pages)
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

    overview_tab, rent_tab, financial_tab, comps_tab, raw_tab, pdf_tab = st.tabs(
        [
            "Overview",
            "Rent vs Market",
            "Financials",
            "Comps",
            "Raw JSON",
            "PDF Preview",
        ]
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
                f"Tokens — prompt: {prompt_tokens}, completion: {completion_tokens}, total: {total_tokens}"
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
            page = st.slider("Page", min_value=1, max_value=len(images), value=1)
            st.image(images[page - 1], use_column_width=True)
            st.caption(f"Showing page {page} of {len(images)}")


if __name__ == "__main__":
    main()