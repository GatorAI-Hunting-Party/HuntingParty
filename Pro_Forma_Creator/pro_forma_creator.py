
import pandas as pd
import openpyxl
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional, Callable
import sys
import pickle

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
om_scraper_path = project_root / "OM_Scraper"

if str(om_scraper_path) not in sys.path:
    sys.path.insert(0, str(om_scraper_path))

from om_extractor import (
    DEFAULT_MAX_PAGES,
    extract_data_from_pdf
)

def estimate_annual_cash_flow(noi: float, debt_service: float) -> float:
    return noi - debt_service

def estimate_monthly_cash_flow(noi: float, debt_service: float) -> float:
    return estimate_annual_cash_flow(noi, debt_service) / 12

#estimate avg_sqft from data["unit_info"]
def estimate_avg_sqft(unit_info: dict[str, dict[str,tuple[float, int, float]]]) -> Optional[float]:
    total_sqft = 0
    total_units = 0
    
    for bed_type, info in unit_info.items():
        avg_sqft = info.get("average_sqft")
        num_units = info.get("number_of_units")
        
        if avg_sqft is not None and num_units is not None:
            total_sqft += avg_sqft * num_units
            total_units += num_units
        
        if total_units <= 0:
            raise ValueError("Total units must be greater than zero to estimate average sqft.")
    
    return total_sqft / total_units

# Prompt user for missing required parameters
params_to_prompts = {
    ## ["unit_info"] ["#_bed"]
    "number_of_units": "What is the total number of units in the property?",
    "average_rent": "What is the average rent per unit?",
    "average_sqft": "What is the average square footage per unit?",

    ## ["location_data"]
    "address": "What is the address of the property?",
    "lot_size": "What is the lot size of the property?",
    "property_age": "What is the age of the property?",
    "year_renovated": "What year was the property last renovated?",
    "rentable_square_footage": "What is the rentable square footage of the property?",
    "oz_status": "Is the property located in an Opportunity Zone? (True/False)",
    "total_units": "What is the total number of units in the property?",
    
    ## ["financials"]
    "noi": "What is the Net Operating Income (NOI) of the property?",
    "cap_rate": "What is the Capitalization Rate (Cap Rate) of the property?",
    "asking_price": "What is the asking price of the property?",
    "expense_ratio": "What is the expense ratio of the property?",
    "expense_cost": "What is the total expense cost of the property?",

    ## ["summary"]
    "summary": "Provide a brief summary of the property."
}

# Missing Params Alternatives
calculate_missing = {
    # All alternatives to calculate missing parameters
    "noi": lambda data: data["asking_price"] * data["cap_rate"],
    "cap_rate": lambda data: data["noi"] / data["asking_price"],
    "asking_price": lambda data: data["noi"] / data["cap_rate"],

    "expense_cost": lambda data: data["asking_price"] * data["expense_ratio"],
    "expense_ratio": lambda data: data["expense_cost"] / data["asking_price"],

    "average_sqft": lambda data: estimate_avg_sqft(data["unit_info"]),
    "rentable_square_footage": lambda data: data["total_units"] * estimate_avg_sqft(data["unit_info"]),
    "total_units": lambda data: sum(x_bed["number_of_units"] for x_bed in data["unit_info"].values()),
}

def handle_missing_parameter(data: dict, parameter: str, prompt_input: Callable):
    is_param_missing = parameter not in data.columns or data[parameter].isnull().all()
    can_calculate = parameter in calculate_missing.keys()


    if can_calculate:
        try:
            calculated_value = calculate_missing[parameter](data)
        except Exception as e:
            print(f"Could not calculate parameter '{parameter}': {e}")

def main():
    # Check if pickle file in project root / Testing / extracted_data.pkl exists, if not, run extraction on sample PDF and save to that location
    if not (project_root / "Testing" / "extracted_data.pkl").exists():
        load_dotenv(Path(__file__).parent / ".env")

        pdf_path = Path(project_root) / "Testing" / "PDFs" / "13_Newfound_St _Brochure.pdf"
        print(f"Extracting data from PDF at: {pdf_path}")

        extracted_data = extract_data_from_pdf(pdf_path=str(pdf_path))

        data = extracted_data["data"] if "data" in extracted_data else None

        #save dat to pickle file
        with open(project_root / "Testing" / "extracted_data.pkl", 'wb') as handle:
            pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)


        if not data:
            print("No data extracted.")
        else:
            print("Extracted Data:")
            print(data)

        # save as pickle file but ensure that it is saved in the format like in the json

    # load pickle file
    extracted_data: dict
    with open(project_root / "Testing" / "extracted_data.pkl", 'rb') as handle:
        extracted_data = pickle.load(handle)


if __name__ == "__main__":
    main()