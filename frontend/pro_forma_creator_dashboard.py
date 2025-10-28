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
if str(om_scraper_path) not in sys.path:
    sys.path.insert(0, str(om_scraper_path))

# Import the extractor module from the OM_Scraper folder (matches comparison_dashboard.py)
from om_extractor import (
    DEFAULT_MAX_PAGES,
    extract_data_from_pdf
)

load_dotenv(Path(__file__).parent / ".env")

st.set_page_config(page_title="OM Extractor", layout="wide")

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

    if uploaded_pdf is None:
        st.info("Upload a PDF Offering Memorandum using the file picker above.")
        return

    # Create a temporary file to store the uploaded PDF
    pdf_path: Optional[str] = None
    temp_path: Optional[Path] = None
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp:
        temp.write(uploaded_pdf.getbuffer())
        temp_path = Path(temp.name)

    pdf_path = str(temp_path)
    source_label = uploaded_pdf.name

    extracted_data: dict = st.session_state.get("om_extraction", None)

    ### ONLY TO SPEED UP TESTING, REMOVE LATER
    if not (project_root / "Testing" / "extracted_data.pkl").exists():
        try:
            with st.spinner("Extracting data from OM..."):
                extracted_data = extract_data_from_pdf(pdf_path=pdf_path)
            st.session_state["om_extraction"] = {
                "data": extracted_data["data"],
                "base64_images": extracted_data["base64_images"],
                "tokens_used": extracted_data.get("tokens_used", {}),
                "source_pdf": source_label
            }
            st.success("Extraction complete.")
        except Exception as exc:
            st.error(f"Extraction failed: {exc}")
        finally:
            if uploaded_pdf is not None and temp_path is not None:
                temp_path.unlink(missing_ok=True)
    else:
        with open(project_root / "Testing" / "extracted_data.pkl", 'rb') as handle:
            extracted_data = pickle.load(handle)
            st.session_state["om_extraction"] = {
                "data": extracted_data["data"],
                "base64_images": extracted_data["base64_images"],
                "tokens_used": extracted_data.get("tokens_used", {}),
                "source_pdf": source_label
            }
    
    
    st.header("Extraction Data")
    if extracted_data:
        extracted_df = pd.DataFrame(extracted_data["data"])
        st.subheader("Extracted Data")
        st.dataframe(extracted_df)
    else:
        st.info("No data extracted yet.")
    
    print(extracted_data["data"] if extracted_data else pd.DataFrame())
    st.dataframe(extracted_data["data"] if extracted_data else pd.DataFrame())

if __name__ == "__main__":
    main()
