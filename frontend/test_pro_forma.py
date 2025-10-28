import sys
from pathlib import Path
from typing import Optional
import tempfile
import streamlit as st
from dotenv import load_dotenv
import pandas as pd
import pickle

current_file = Path(__file__).resolve()
frontend_dir = current_file.parent
project_root = frontend_dir.parent
om_scraper_path = project_root / "OM_Scraper"
pro_forma_creator_path = project_root / "Pro_Forma_Creator"

if str(om_scraper_path) not in sys.path:
    sys.path.insert(0, str(om_scraper_path))
if str(pro_forma_creator_path) not in sys.path:
    sys.path.insert(0, str(pro_forma_creator_path))

# Import the extractor module from the OM_Scraper folder (matches comparison_dashboard.py)
from om_extractor import (
    DEFAULT_MAX_PAGES,
    extract_data_from_pdf
)

from pro_forma_creator import calculate_missing

load_dotenv(Path(__file__).parent / ".env")

st.set_page_config(page_title="OM Extractor", layout="wide")

def get_as(value: any, default: any, val_cast: type = None) -> any:
    if value is None:
        if val_cast is None:
            return default
        
        return val_cast(default)
    
    if val_cast is None:
        return value

    return val_cast(value)

def main():
    st.title("Offering Memorandum (OM) Extractor")
    st.caption("Extract property data from Offering Memorandums using Azure OpenAI Vision.")

    col1, col2 = st.columns([3, 1])
    
    uploaded_pdf = st.file_uploader(
        "Upload an Offering Memorandum PDF",
        type=["pdf"],
        accept_multiple_files=False,
        help="The PDF will be processed using Azure OpenAI Vision to extract property details."
    )

    
    with open(project_root / "Testing" / "extracted_data.pkl", 'rb') as handle:
        st.session_state["om_extraction"] = {"data": pickle.load(handle)}

    ### ONLY TO SPEED UP TESTING, REMOVE LATER
    if not (project_root / "Testing" / "extracted_data.pkl").exists():
        print("PICKE FILE NOT FOUND")
        raise FileNotFoundError("Pickle file with extracted data not found for testing.")
    
    def get_financial_value(key: str):
        return st.session_state["om_extraction"]["data"]["financials"].get(key, None)
    
    def set_financial_value(key: str, value: float|None):
        st.session_state["om_extraction"]["data"]["financials"][key] = value

    def current_financials():
        return [
            get_financial_value("noi"),
            get_financial_value("expense_ratio"),
            get_financial_value("cap_rate"),
            get_financial_value("asking_price"),
            get_financial_value("expense_cost"),
        ]
    
    if get_financial_value("cap_rate") is not None and get_financial_value("cap_rate") > 1.0:
        print("Converting cap rate from percentage to decimal.")
        st.session_state["om_extraction"]["data"]["financials"]["cap_rate"] /= 100.0

    updated = True
    while (updated):
        updated = False
        for param_name in ["noi", "expense_ratio", "cap_rate", "asking_price", "expense_cost"]:
            param_value = get_financial_value(param_name)
            can_calculate = param_name in calculate_missing.keys()
            if (param_value is None or param_value == 0.0) and can_calculate:
                try:
                    calculated_value = calculate_missing[param_name](st.session_state["om_extraction"]["data"]["financials"])
                    st.session_state["om_extraction"]["data"]["financials"][param_name] = calculated_value
                    updated = True
                except Exception as e:
                    pass

    st.header("Extraction Data")   
    st.subheader("Financials")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Annual NOI ($)", f"{get_as(get_financial_value('noi'), 0.0, float):,.2f}")
        st.metric("Asking Price ($)", f"{get_as(get_financial_value('asking_price'), 0.0, float):,.2f}")
    with col2:
        st.metric("Expense Ratio (%)", f"{get_as(get_financial_value('expense_ratio'), 0.0, float):,.3f}")
        st.metric("Expense Cost ($)", f"{get_as(get_financial_value('expense_cost'), 0.0, float):,.2f}")
    with col3:
        st.metric("Cap Rate (%)", f"{get_as(get_financial_value('cap_rate'), 0.0, float):,.3f}")

    
    params = [noi, expense_ratio, cap_rate, asking_price, expense_cost] = current_financials()

    if any(p is None or p == 0.0 for p in params):
        st.warning("Some financial values are missing or zero. Please provide the missing values below to update the extraction data.", icon="⚠️")
        with st.form("financial_inputs"):
            # Create input fields with extracted values
            if not noi or noi == 0.0:
                noi = st.number_input("Enter NOI ($)", value=0.0, step=1000.0)
            if not expense_ratio or expense_ratio == 0.0:
                expense_ratio = st.number_input("Enter Expense Ratio (%)", value=0.0, step=0.1)
            if not cap_rate or cap_rate == 0.0:
                cap_rate = st.number_input("Enter Cap Rate (%)", value=0.0, step=0.1)
            if not asking_price or asking_price == 0.0:
                asking_price = st.number_input("Enter Asking Price ($)", value=0.0, step=1000.0)
            if not expense_cost or expense_cost == 0.0:
                expense_cost = st.number_input("Enter Expense Cost ($)", value=0.0, step=1000.0)

            if st.form_submit_button("Update Financials"):
                st.session_state["om_extraction"]["data"]["financials"]["noi"]           = noi
                st.session_state["om_extraction"]["data"]["financials"]["expense_ratio"] = expense_ratio
                st.session_state["om_extraction"]["data"]["financials"]["cap_rate"]      = cap_rate
                st.session_state["om_extraction"]["data"]["financials"]["asking_price"]  = asking_price
                st.session_state["om_extraction"]["data"]["financials"]["expense_cost"]  = expense_cost

if __name__ == "__main__":
    main()
