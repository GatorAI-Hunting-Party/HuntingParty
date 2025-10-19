from __future__ import annotations

import base64
import io
import json
import os
from typing import Any, Dict, List, Sequence

import requests
from pdf2image import convert_from_path
from PIL import Image

DEFAULT_DPI = 150
DEFAULT_MAX_PAGES = 12

AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")


EXTRACTION_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "unit_info": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "1_bed": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "average_rent": {"type": ["number", "null"]},
                        "number_of_units": {"type": ["integer", "null"]},
                        "average_sqft": {"type": ["number", "null"]},
                    },
                    "required": ["average_rent", "number_of_units", "average_sqft"],
                },
                "2_bed": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "average_rent": {"type": ["number", "null"]},
                        "number_of_units": {"type": ["integer", "null"]},
                        "average_sqft": {"type": ["number", "null"]},
                    },
                    "required": ["average_rent", "number_of_units", "average_sqft"],
                },
                "3_bed": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "average_rent": {"type": ["number", "null"]},
                        "number_of_units": {"type": ["integer", "null"]},
                        "average_sqft": {"type": ["number", "null"]},
                    },
                    "required": ["average_rent", "number_of_units", "average_sqft"],
                },
                "4_bed": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "average_rent": {"type": ["number", "null"]},
                        "number_of_units": {"type": ["integer", "null"]},
                        "average_sqft": {"type": ["number", "null"]},
                    },
                    "required": ["average_rent", "number_of_units", "average_sqft"],
                },
            },
            "required": ["1_bed", "2_bed", "3_bed", "4_bed"],
        },
        "location_data": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "address": {"type": ["string", "null"]},
                "lot_size": {"type": ["string", "null"]},
                "property_age": {"type": ["integer", "null"]},
                "year_renovated": {"type": ["integer", "null"]},
                "rentable_square_footage": {"type": ["number", "null"]},
                "oz_status": {"type": ["boolean", "null"]},
                "total_units": {"type": ["integer", "null"]},
            },
            "required": [
                "address",
                "lot_size",
                "property_age",
                "year_renovated",
                "rentable_square_footage",
                "oz_status",
                "total_units",
            ],
        },
        "financials": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "noi": {"type": ["number", "null"]},
                "cap_rate": {"type": ["number", "null"]},
                "asking_price": {"type": ["number", "null"]},
                "expense_ratio": {"type": ["number", "null"]},
                "expense_cost": {"type": ["number", "null"]},
            },
            "required": ["noi", "cap_rate", "asking_price", "expense_ratio", "expense_cost"],
        },
        "summary": {
            "type": "string",
        },
    },
    "required": ["unit_info", "location_data", "financials", "summary"],
}

SYSTEM_PROMPT = """You are an expert real estate analyst specializing in commercial real estate and multifamily property offering memorandums (OMs).

Extract the following information from the property documents:

**Unit Information (for 1, 2, 3, and 4 bedroom units):**
- Average monthly rent
- Number of units
- Average square footage (if available)

**Location Data:**
- Property address (full address with city, state, zip)
- Lot size (with units)
- Property age or year built
- Year renovated (if mentioned)
- Rentable square footage (RSF/GLA/GBA)
- Opportunity Zone (OZ) status
- Total number of units

**Financials:**
- NOI (Net Operating Income)
- Cap rate (capitalization rate)
- Asking price / sale price
- Expense ratio or total operating expenses (if available)

**Summary:**
- Create a concise 2-3 sentence property summary

**Important Instructions:**
- If a field is not found in the document, return null for that field
- For unit counts, if you see a rent roll table, count the actual units
- Different documents may use different terminology (e.g., "GLA" vs "Rentable SF" vs "Building Area")
- Look for unit mix tables, rent rolls, financial summaries, and property highlights
- Cap rate might be explicitly stated or you may need to calculate it from NOI and price
- Be flexible with field names - they vary significantly across OMs"""

def pdf_to_images(pdf_path: str, *, max_pages: int = DEFAULT_MAX_PAGES, dpi: int = DEFAULT_DPI) -> List[Image.Image]:
    return convert_from_path(pdf_path, dpi=dpi, first_page=1, last_page=max_pages)


def images_to_base64(images: Sequence[Image.Image], *, format: str = "JPEG", quality: int = 85) -> List[str]:
    encoded: List[str] = []
    for image in images:
        buffer = io.BytesIO()
        image.save(buffer, format=format, quality=quality)
        encoded.append(base64.b64encode(buffer.getvalue()).decode("utf-8"))
    return encoded


def _build_message_content(base64_images: Sequence[str]) -> List[Dict[str, Any]]:
    content: List[Dict[str, Any]] = [{"type": "text", "text": SYSTEM_PROMPT}]
    for img in base64_images:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{img}"}})
    return content


def call_azure_extraction(
    base64_images: Sequence[str],
    *,
    max_tokens: int = 4000,
    temperature: float = 0.1,
    timeout: int = 60,
) -> Dict[str, Any]:
    if not AZURE_ENDPOINT:
        raise RuntimeError("Azure OpenAI endpoint is not configured.")
    if not AZURE_API_KEY:
        raise RuntimeError("Azure OpenAI API key is not configured.")

    payload = {
        "messages": [
            {"role": "user", "content": _build_message_content(base64_images)}
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "property_extraction",
                "strict": True,
                "schema": EXTRACTION_SCHEMA,
            },
        },
    }

    headers = {"Content-Type": "application/json", "api-key": AZURE_API_KEY}

    response = requests.post(AZURE_ENDPOINT, headers=headers, json=payload, timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError(f"Azure OpenAI request failed ({response.status_code}): {response.text}")

    result = response.json()
    message = result["choices"][0]["message"]["content"]
    data = json.loads(message)

    return {"data": data, "raw": result, "tokens_used": result.get("usage", {})}


def extract_data_from_pdf(
    pdf_path: str,
    *,
    max_pages: int = DEFAULT_MAX_PAGES,
    dpi: int = DEFAULT_DPI,
    max_tokens: int = 4000,
    temperature: float = 0.1,
) -> Dict[str, Any]:
    images = pdf_to_images(pdf_path, max_pages=max_pages, dpi=dpi)
    base64_images = images_to_base64(images)
    extraction = call_azure_extraction(base64_images, max_tokens=max_tokens, temperature=temperature)
    extraction["base64_images"] = base64_images
    return extraction


__all__ = [
    "AZURE_ENDPOINT",
    "AZURE_API_KEY",
    "DEFAULT_DPI",
    "DEFAULT_MAX_PAGES",
    "EXTRACTION_SCHEMA",
    "SYSTEM_PROMPT",
    "pdf_to_images",
    "images_to_base64",
    "call_azure_extraction",
    "extract_data_from_pdf",
]




