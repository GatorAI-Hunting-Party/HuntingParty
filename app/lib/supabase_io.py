from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from .secrets import get_supabase_config

try:
    import streamlit as st

    cache_resource = st.cache_resource
    cache_data = st.cache_data
except Exception:

    def cache_resource(func=None, **_):
        if func is None:
            def decorator(inner):
                return inner

            return decorator
        return func

    def cache_data(func=None, **_):
        if func is None:
            def decorator(inner):
                return inner

            return decorator
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
def fetch_crexi_comps(
    city: Optional[str] = None, state: Optional[str] = None, limit: int = 1000
) -> pd.DataFrame:
    sb = get_supabase_client()
    q = sb.table("crexi_merged_ny_clean").select("*")
    if city:
        q = q.eq("City", city)
    if state:
        q = q.eq("State", state)
    q = q.limit(limit)
    return _df(q.execute().data)


@cache_data(show_spinner=False, ttl=300)
def fetch_realtor_props(
    city: Optional[str] = None, state: Optional[str] = None, limit: int = 5000
) -> pd.DataFrame:
    sb = get_supabase_client()
    q = sb.table("realtor_properties_ny_clean").select("*")
    if city:
        q = q.eq("city", city)
    if state:
        q = q.eq("state", state)
    q = q.limit(limit)
    return _df(q.execute().data)


@cache_data(show_spinner=False, ttl=300)
def fetch_realtor_rent(
    city: Optional[str] = None, state: Optional[str] = None, limit: int = 5000
) -> pd.DataFrame:
    sb = get_supabase_client()
    q = sb.table("realtor_rent_clean").select("*")
    if city:
        q = q.eq("city", city)
    if state:
        q = q.eq("state", state)
    q = q.limit(limit)
    return _df(q.execute().data)


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
