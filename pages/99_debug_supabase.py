import streamlit as st

from app.lib.supabase_io import fetch_crexi, fetch_realtor_props, fetch_market_medians

st.title("Supabase Connectivity Debug")
city = st.text_input("City", value="Brooklyn")
state = st.text_input("State", value="NY")
if st.button("Run test queries"):
    st.write("CREXi:", fetch_crexi(city, state, 5))
    st.write("Realtor props:", fetch_realtor_props(city, state, 5))
    st.write("Medians:", fetch_market_medians(f"{city}, {state}").head())
st.caption("Reads SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) from Streamlit Secrets.")
