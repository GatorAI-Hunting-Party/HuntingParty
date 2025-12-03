import streamlit as st
import pandas as pd
import pydeck as pdk

from app.lib.supabase_io import supabase_ping, fetch_crexi_with_fallback, fetch_realtor_props_with_fallback
from app.gwr.pipeline import gwr_surface, run_teammate_gwr


st.title("GWR Map (Beta)")
ok_ping, ping_msg = supabase_ping()
st.caption("Supabase-backed. Falls back to kernel-weighted local regression if full GWR pipeline is unavailable.")
st.info(f"Supabase ping: {'OK' if ok_ping else 'ERROR'} | {ping_msg}")
if not ok_ping:
    st.error("Supabase is not reachable; map rendering may fail.")

city = st.text_input("City", value=st.session_state.get("om_city", "Brooklyn"))
state = st.text_input("State", value=st.session_state.get("om_state", "NY"))
provider = st.selectbox("Provider", ["CREXi (price/sf)", "Realtor (price/sf)"])
bandwidth = st.slider("Bandwidth (miles)", 2.0, 25.0, 8.0, 0.5)
grid_step = st.slider("Grid step (miles)", 0.5, 10.0, 2.0, 0.5)

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
    st.write(f"Samples: {len(df)}")

    surf = run_teammate_gwr(df, metric_col=metric_col, lat_col=lat_col, lon_col=lon_col)
    if surf.empty:
        surf = gwr_surface(
            df,
            metric_col=metric_col,
            lat_col=lat_col,
            lon_col=lon_col,
            bandwidth_mi=bandwidth,
            grid_step_mi=grid_step,
        )

    if surf.empty:
        st.warning("Not enough data to compute surface.")
        st.stop()

    mid_lat = float(surf["lat"].mean())
    mid_lon = float(surf["lon"].mean())
    pmin, pmax = surf["pred"].min(), surf["pred"].max()
    surf["norm"] = (surf["pred"] - pmin) / (pmax - pmin + 1e-9)
    color_range = [
        [49, 54, 149],
        [69, 117, 180],
        [116, 173, 209],
        [171, 217, 233],
        [224, 243, 248],
        [254, 224, 144],
        [253, 174, 97],
        [244, 109, 67],
        [215, 48, 39],
        [165, 0, 38],
    ]
    layer = pdk.Layer(
        "HeatmapLayer",
        data=surf.dropna(subset=["lat", "lon", "pred"]),
        get_position="[lon, lat]",
        get_weight="pred",
        aggregation="MEAN",
        radius_pixels=50,
        color_range=color_range,
    )
    tooltip = {
        "html": "<b>Predicted $/sf:</b> {pred}<br/><b>Samples used:</b> {n_used}",
        "style": {"backgroundColor": "steelblue", "color": "white"},
    }
    view_state = pdk.ViewState(latitude=mid_lat, longitude=mid_lon, zoom=10)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip=tooltip))
    st.caption(f"Predicted $/sf range: {pmin:,.0f} – {pmax:,.0f}. Heatmap weighted by predicted price/sf; warmer colors = higher.")
except Exception as e:
    st.error(f"GWR map error: {e}")
