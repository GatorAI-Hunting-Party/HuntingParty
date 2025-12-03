from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .secrets import get_supabase_config

try:
    import streamlit as st

    cache_resource = st.cache_resource
    cache_data = st.cache_data
except Exception:

    def cache_resource(func=None, **_):
        if func is None:
            return lambda inner: inner
        return func

    def cache_data(func=None, **_):
        if func is None:
            return lambda inner: inner
        return func


@cache_resource(show_spinner=False)
def get_supabase_client():
    from supabase import create_client

    cfg = get_supabase_config()
    if not cfg["url"] or not cfg["key"]:
        raise RuntimeError(
            "Supabase credentials missing. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) in Streamlit Secrets."
        )
    return create_client(cfg["url"], cfg["key"])


def _df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows or [])


@cache_data(show_spinner=False, ttl=300)
def fetch_crexi(
    city: Optional[str] = None, state: Optional[str] = None, limit: int = 5000
) -> pd.DataFrame:
    sb = get_supabase_client()
    q = sb.table("crexi_merged_ny_clean").select("*")
    if city:
        q = q.eq("City", city.strip().title())
    if state:
        q = q.eq("State", state.strip().upper())
    return _df(q.limit(limit).execute().data)


@cache_data(show_spinner=False, ttl=300)
def fetch_realtor_props(
    city: Optional[str] = None, state: Optional[str] = None, limit: int = 5000
) -> pd.DataFrame:
    sb = get_supabase_client()
    q = sb.table("realtor_properties_ny_clean").select("*")
    if city:
        q = q.eq("city", city.strip().title())
    if state:
        q = q.eq("state", state.strip().upper())
    return _df(q.limit(limit).execute().data)


@cache_data(show_spinner=False, ttl=300)
def fetch_realtor_rent(
    city: Optional[str] = None, state: Optional[str] = None, limit: int = 5000
) -> pd.DataFrame:
    sb = get_supabase_client()
    q = sb.table("realtor_rent_clean").select("*")
    if city:
        q = q.eq("city", city.strip().title())
    if state:
        q = q.eq("state", state.strip().upper())
    return _df(q.limit(limit).execute().data)


@cache_data(show_spinner=False, ttl=300)
def fetch_market_medians(
    geography: Optional[str] = None,
    metrics: Optional[List[str]] = None,
    asset_type: Optional[str] = None,
) -> pd.DataFrame:
    sb = get_supabase_client()
    q = sb.table("market_medians_all").select("*")
    if geography:
        q = q.eq("geography", geography)
    if asset_type:
        q = q.eq("asset_type", asset_type)
    if metrics:
        q = q.in_("metric_name", metrics)
    return _df(q.execute().data)


@cache_data(show_spinner=False, ttl=300)
def list_geographies() -> pd.DataFrame:
    sb = get_supabase_client()
    res = sb.table("market_medians_all").select("geography, asset_type, comps_count").execute()
    return _df(res.data)


def _norm_city_state(city: Optional[str], state: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    c = city.strip().title() if city else None
    s = state.strip().upper() if state else None
    return c, s


@cache_data(show_spinner=False, ttl=300)
def fetch_crexi_with_fallback(city: Optional[str], state: Optional[str], limit: int = 5000) -> Tuple[pd.DataFrame, str]:
    sb = get_supabase_client()
    c, s = _norm_city_state(city, state)
    q1 = sb.table("crexi_merged_ny_clean").select("*")
    if c:
        q1 = q1.eq("City", c)
    if s:
        q1 = q1.eq("State", s)
    d1 = _df(q1.limit(limit).execute().data)
    if not d1.empty:
        return d1, f"{c}, {s}" if c and s else (c or s or "global")

    if s:
        d2 = _df(sb.table("crexi_merged_ny_clean").select("*").eq("State", s).limit(limit).execute().data)
        if not d2.empty:
            return d2, f"{s} (state)"

    d3 = _df(sb.table("crexi_merged_ny_clean").select("*").limit(limit).execute().data)
    return d3, "global"


@cache_data(show_spinner=False, ttl=300)
def fetch_realtor_props_with_fallback(city: Optional[str], state: Optional[str], limit: int = 5000) -> Tuple[pd.DataFrame, str]:
    sb = get_supabase_client()
    c, s = _norm_city_state(city, state)
    q1 = sb.table("realtor_properties_ny_clean").select("*")
    if c:
        q1 = q1.eq("city", c)
    if s:
        q1 = q1.eq("state", s)
    d1 = _df(q1.limit(limit).execute().data)
    if not d1.empty:
        return d1, f"{c}, {s}" if c and s else (c or s or "global")

    if s:
        d2 = _df(sb.table("realtor_properties_ny_clean").select("*").eq("state", s).limit(limit).execute().data)
        if not d2.empty:
            return d2, f"{s} (state)"

    d3 = _df(sb.table("realtor_properties_ny_clean").select("*").limit(limit).execute().data)
    return d3, "global"
