"""
Hunting Party Backend API
Real Estate Investment Analysis System

Simple API for processing Offering Memorandums and comparing against market data.
"""

import os
import csv
import io
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(supabase_url, supabase_key) if supabase_url and supabase_key else None

# Initialize FastAPI app
app = FastAPI(title="Hunting Party API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class PropertySummary:
    address: str
    opportunity_zone: bool
    cap_rate: float
    rentable_sqft: int
    avg_sqft_per_unit: float
    asking_price: float
    lot_size_acres: float
    total_units: int

@dataclass
class UnitRentData:
    one_bed: Optional[float] = None
    two_bed: Optional[float] = None
    three_bed: Optional[float] = None
    four_bed: Optional[float] = None

@dataclass
class FinancialMetrics:
    noi: float
    cap_rate: float
    price_per_sqft: float
    price_per_unit: float
    price_per_acre: float

@dataclass
class VacancyEGIData:
    vacancy_rate: float
    gross_potential_rent: float
    effective_gross_income: float

@dataclass
class CompProperty:
    source: str  # "crexi" or "realtor"
    address: str
    price: Optional[float] = None
    units: Optional[int] = None
    cap_rate: Optional[float] = None
    price_per_unit: Optional[float] = None
    price_per_sqft: Optional[float] = None
    price_per_acre: Optional[float] = None
    noi: Optional[float] = None
    distance_miles: Optional[float] = None
    unit_rents: Optional[UnitRentData] = None

@dataclass
class CompStats:
    total_scraped: int
    filtered_count: int
    avg_price: float
    avg_units: float
    avg_cap_rate: Optional[float]
    avg_price_per_unit: float
    avg_price_per_sqft: float
    avg_price_per_acre: Optional[float]
    avg_noi: Optional[float]
    avg_unit_rents: Optional[UnitRentData] = None

@dataclass
class ComparisonResult:
    metric_name: str
    om_value: Optional[float]
    market_value: Optional[float]
    deviation_percent: Optional[float]
    deviation_comment: str

# ============================================================================
# API MODELS
# ============================================================================

class CompFilterRequest(BaseModel):
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    units_min: Optional[int] = None
    units_max: Optional[int] = None
    distance_miles: Optional[float] = None
    cap_rate_min: Optional[float] = None
    cap_rate_max: Optional[float] = None
    price_per_unit_min: Optional[float] = None
    price_per_unit_max: Optional[float] = None
    price_per_sqft_min: Optional[float] = None
    price_per_sqft_max: Optional[float] = None
    price_per_acre_min: Optional[float] = None
    price_per_acre_max: Optional[float] = None

class AnalysisResponse(BaseModel):
    property_summary: Dict[str, Any]
    unit_rents: Dict[str, Any]
    financials: Dict[str, Any]
    vacancy_egi: Dict[str, Any]
    crexi_comps: Dict[str, Any]
    realtor_comps: Dict[str, Any]
    comparisons: List[Dict[str, Any]]
    raw_crexi_comps: List[Dict[str, Any]]
    raw_realtor_comps: List[Dict[str, Any]]

# ============================================================================
# CSV PROCESSING AND DATA SERVICES
# ============================================================================

def parse_csv_om_data(csv_content: str) -> Dict[str, Any]:
    """Parse CSV content and extract OM data"""
    try:
        reader = csv.DictReader(io.StringIO(csv_content))
        row = next(reader)  # Get first row
        
        # Extract property summary
        property_summary = PropertySummary(
            address=row.get('address', ''),
            opportunity_zone=row.get('opportunity_zone', '').lower() == 'yes',
            cap_rate=float(row.get('cap_rate', 0)),
            rentable_sqft=int(float(row.get('rentable_sqft', 0))),
            avg_sqft_per_unit=float(row.get('avg_sqft_per_unit', 0)),
            asking_price=float(row.get('asking_price', 0)),
            lot_size_acres=float(row.get('lot_size_acres', 0)),
            total_units=int(float(row.get('total_units', 0)))
        )
        
        # Extract unit rent data
        unit_rents = UnitRentData(
            one_bed=float(row.get('one_bed_rent', 0)) if row.get('one_bed_rent') else None,
            two_bed=float(row.get('two_bed_rent', 0)) if row.get('two_bed_rent') else None,
            three_bed=float(row.get('three_bed_rent', 0)) if row.get('three_bed_rent') else None,
            four_bed=float(row.get('four_bed_rent', 0)) if row.get('four_bed_rent') else None
        )
        
        # Extract financial metrics
        financials = FinancialMetrics(
            noi=float(row.get('noi', 0)),
            cap_rate=float(row.get('cap_rate', 0)),
            price_per_sqft=float(row.get('price_per_sqft', 0)),
            price_per_unit=float(row.get('price_per_unit', 0)),
            price_per_acre=float(row.get('price_per_acre', 0))
        )
        
        # Extract vacancy and EGI data
        vacancy_rate = float(row.get('vacancy_rate', 0))
        gross_potential_rent = float(row.get('gross_potential_rent', 0))
        vacancy_egi = VacancyEGIData(
            vacancy_rate=vacancy_rate,
            gross_potential_rent=gross_potential_rent,
            effective_gross_income=gross_potential_rent * (1 - vacancy_rate)
        )
        
        return {
            "property_summary": property_summary,
            "unit_rents": unit_rents,
            "financials": financials,
            "vacancy_egi": vacancy_egi
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error parsing CSV: {str(e)}")

def get_supabase_market_data(geography: str, asset_type: str = "Multifamily") -> Dict[str, List[CompProperty]]:
    """Get market data from Supabase"""
    if not supabase:
        return {"crexi": [], "realtor": []}
    
    try:
        # Get CREXi market data
        crexi_response = supabase.table("market_metrics").select("*").eq("provider", "CREXi").eq("geography", geography).eq("asset_type", asset_type).execute()
        
        crexi_comps = []
        if crexi_response.data:
            for record in crexi_response.data:
                crexi_comps.append(CompProperty(
                    source="crexi",
                    address=f"Market data for {geography}",
                    price=float(record.get('value', 0)) if record.get('metric_name') == 'asking_price' else None,
                    units=None,
                    cap_rate=float(record.get('value', 0)) if record.get('metric_name') == 'cap_rate_pct' else None,
                    price_per_unit=float(record.get('value', 0)) if record.get('metric_name') == 'price_per_unit_usd' else None,
                    price_per_sqft=float(record.get('value', 0)) if record.get('metric_name') == 'price_per_sf_usd' else None,
                    price_per_acre=float(record.get('value', 0)) if record.get('metric_name') == 'price_per_acre_usd' else None,
                    noi=float(record.get('value', 0)) if record.get('metric_name') == 'noi_usd' else None,
                    distance_miles=None
                ))
        
        # Get Realtor.com market data
        realtor_response = supabase.table("market_metrics").select("*").eq("provider", "Realtor.com").eq("geography", geography).eq("asset_type", asset_type).execute()
        
        realtor_comps = []
        if realtor_response.data:
            for record in realtor_response.data:
                realtor_comps.append(CompProperty(
                    source="realtor",
                    address=f"Market data for {geography}",
                    price=None,
                    units=None,
                    cap_rate=None,
                    price_per_unit=None,
                    price_per_sqft=None,
                    price_per_acre=None,
                    noi=None,
                    distance_miles=None,
                    unit_rents=UnitRentData(
                        one_bed=float(record.get('value', 0)) if record.get('metric_name') == 'avg_rent_usd_month' and record.get('unit_type') == '1BR' else None,
                        two_bed=float(record.get('value', 0)) if record.get('metric_name') == 'avg_rent_usd_month' and record.get('unit_type') == '2BR' else None,
                        three_bed=float(record.get('value', 0)) if record.get('metric_name') == 'avg_rent_usd_month' and record.get('unit_type') == '3BR' else None,
                        four_bed=float(record.get('value', 0)) if record.get('metric_name') == 'avg_rent_usd_month' and record.get('unit_type') == '4BR' else None
                    )
                ))
        
        return {"crexi": crexi_comps, "realtor": realtor_comps}
        
    except Exception as e:
        print(f"Error fetching from Supabase: {str(e)}")
        return {"crexi": [], "realtor": []}

def get_mock_om_data():
    """Get mock OM data"""
    return {
        "property_summary": PropertySummary(
            address="123 Main St, Tampa, FL",
            opportunity_zone=False,
            cap_rate=5.4,
            rentable_sqft=108750,
            avg_sqft_per_unit=920,
            asking_price=34250000,
            lot_size_acres=1.85,
            total_units=120
        ),
        "unit_rents": UnitRentData(
            one_bed=1650, two_bed=2100, three_bed=2450, four_bed=2800
        ),
        "financials": FinancialMetrics(
            noi=1890000, cap_rate=5.4, price_per_sqft=315,
            price_per_unit=285417, price_per_acre=18513514
        ),
        "vacancy_egi": VacancyEGIData(
            vacancy_rate=0.06, gross_potential_rent=2300000,
            effective_gross_income=2162000
        )
    }

def get_mock_crexi_comps():
    """Get mock CREXi comparable properties"""
    return [
        CompProperty(
            source="crexi", address="456 Oak St, Tampa, FL", price=33500000,
            units=115, cap_rate=5.5, price_per_unit=291304, price_per_sqft=312,
            price_per_acre=18200000, noi=1842500, distance_miles=0.8
        ),
        CompProperty(
            source="crexi", address="789 Pine Ave, Tampa, FL", price=32000000,
            units=108, cap_rate=5.3, price_per_unit=296296, price_per_sqft=318,
            price_per_acre=19000000, noi=1696000, distance_miles=1.2
        ),
        CompProperty(
            source="crexi", address="321 Elm Dr, Tampa, FL", price=35500000,
            units=125, cap_rate=5.4, price_per_unit=284000, price_per_sqft=309,
            price_per_acre=18800000, noi=1917000, distance_miles=0.5
        )
    ]

def extract_geography_from_address(address: str) -> str:
    """Extract geography (city, state) from address"""
    # Simple extraction - look for city, state pattern
    parts = address.split(',')
    if len(parts) >= 2:
        return f"{parts[-2].strip()}, {parts[-1].strip()}"
    return "Tampa, FL"  # Default fallback

def apply_filters(comps: List[CompProperty], filters: CompFilterRequest) -> List[CompProperty]:
    """Apply filters to comparable properties"""
    filtered_comps = []
    for comp in comps:
        # Apply price filters
        if filters.price_min and comp.price and comp.price < filters.price_min:
            continue
        if filters.price_max and comp.price and comp.price > filters.price_max:
            continue
        
        # Apply unit filters
        if filters.units_min and comp.units and comp.units < filters.units_min:
            continue
        if filters.units_max and comp.units and comp.units > filters.units_max:
            continue
        
        # Apply distance filters
        if filters.distance_miles and comp.distance_miles and comp.distance_miles > filters.distance_miles:
            continue
        
        # Apply cap rate filters
        if filters.cap_rate_min and comp.cap_rate and comp.cap_rate < filters.cap_rate_min:
            continue
        if filters.cap_rate_max and comp.cap_rate and comp.cap_rate > filters.cap_rate_max:
            continue
        
        # Apply price per unit filters
        if filters.price_per_unit_min and comp.price_per_unit and comp.price_per_unit < filters.price_per_unit_min:
            continue
        if filters.price_per_unit_max and comp.price_per_unit and comp.price_per_unit > filters.price_per_unit_max:
            continue
        
        # Apply price per sqft filters
        if filters.price_per_sqft_min and comp.price_per_sqft and comp.price_per_sqft < filters.price_per_sqft_min:
            continue
        if filters.price_per_sqft_max and comp.price_per_sqft and comp.price_per_sqft > filters.price_per_sqft_max:
            continue
        
        # Apply price per acre filters
        if filters.price_per_acre_min and comp.price_per_acre and comp.price_per_acre < filters.price_per_acre_min:
            continue
        if filters.price_per_acre_max and comp.price_per_acre and comp.price_per_acre > filters.price_per_acre_max:
            continue
        
        filtered_comps.append(comp)
    
    return filtered_comps

def store_om_data_in_supabase(om_data: Dict[str, Any]):
    """Store OM data in Supabase investor_metrics table"""
    if not supabase:
        return
    
    try:
        # Store property summary as investor metrics
        property_summary = om_data['property_summary']
        financials = om_data['financials']
        unit_rents = om_data['unit_rents']
        
        # Generate a unique deal_id
        deal_id = f"OM_{property_summary.address.replace(' ', '_').replace(',', '')}"
        
        # Prepare data for investor_metrics table
        metrics_to_store = [
            {
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "rentable_sqft",
                "value_num": property_summary.rentable_sqft,
                "unit": "sqft"
            },
            {
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "cap_rate_pct",
                "value_num": financials.cap_rate,
                "unit": "%"
            },
            {
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "noi_usd",
                "value_num": financials.noi,
                "unit": "USD"
            },
            {
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "price_per_sf_usd",
                "value_num": financials.price_per_sqft,
                "unit": "$/sf"
            },
            {
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "price_per_unit_usd",
                "value_num": financials.price_per_unit,
                "unit": "$/unit"
            },
            {
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "price_per_acre_usd",
                "value_num": financials.price_per_acre,
                "unit": "$/acre"
            }
        ]
        
        # Add unit rent data if available
        if unit_rents.one_bed:
            metrics_to_store.append({
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "avg_rent_usd_month",
                "unit_type": "1BR",
                "value_num": unit_rents.one_bed,
                "unit": "USD/month"
            })
        
        if unit_rents.two_bed:
            metrics_to_store.append({
                "deal_id": deal_id,
                "geography": extract_geography_from_address(property_summary.address),
                "asset_type": "Multifamily",
                "metric_name": "avg_rent_usd_month",
                "unit_type": "2BR",
                "value_num": unit_rents.two_bed,
                "unit": "USD/month"
            })
        
        # Insert data into Supabase
        supabase.table("investor_metrics").insert(metrics_to_store).execute()
        
    except Exception as e:
        print(f"Error storing OM data in Supabase: {str(e)}")

def get_mock_realtor_comps():
    """Get mock Realtor.com comparable properties"""
    return [
        CompProperty(
            source="realtor", address="654 Maple St, Tampa, FL", distance_miles=0.9,
            unit_rents=UnitRentData(one_bed=1525, two_bed=1943, three_bed=2263, four_bed=2538)
        ),
        CompProperty(
            source="realtor", address="987 Cedar Rd, Tampa, FL", distance_miles=1.1,
            unit_rents=UnitRentData(one_bed=1580, two_bed=1980, three_bed=2320, four_bed=2580)
        ),
        CompProperty(
            source="realtor", address="147 Birch Ln, Tampa, FL", distance_miles=0.7,
            unit_rents=UnitRentData(one_bed=1480, two_bed=1920, three_bed=2240, four_bed=2520)
        )
    ]

def calculate_comp_stats(comps: List[CompProperty]) -> CompStats:
    """Calculate statistics for comparable properties"""
    if not comps:
        return CompStats(0, 0, 0, 0, None, 0, 0, None, None, None)
    
    # Calculate averages
    prices = [c.price for c in comps if c.price is not None]
    units = [c.units for c in comps if c.units is not None]
    cap_rates = [c.cap_rate for c in comps if c.cap_rate is not None]
    price_per_units = [c.price_per_unit for c in comps if c.price_per_unit is not None]
    price_per_sqfts = [c.price_per_sqft for c in comps if c.price_per_sqft is not None]
    price_per_acres = [c.price_per_acre for c in comps if c.price_per_acre is not None]
    nois = [c.noi for c in comps if c.noi is not None]
    
    # Calculate average unit rents
    unit_rents = [c.unit_rents for c in comps if c.unit_rents is not None]
    avg_unit_rents = None
    if unit_rents:
        one_beds = [u.one_bed for u in unit_rents if u.one_bed is not None]
        two_beds = [u.two_bed for u in unit_rents if u.two_bed is not None]
        three_beds = [u.three_bed for u in unit_rents if u.three_bed is not None]
        four_beds = [u.four_bed for u in unit_rents if u.four_bed is not None]
        
        avg_unit_rents = UnitRentData(
            one_bed=sum(one_beds) / len(one_beds) if one_beds else None,
            two_bed=sum(two_beds) / len(two_beds) if two_beds else None,
            three_bed=sum(three_beds) / len(three_beds) if three_beds else None,
            four_bed=sum(four_beds) / len(four_beds) if four_beds else None
        )
    
    return CompStats(
        total_scraped=len(comps),
        filtered_count=len(comps),
        avg_price=sum(prices) / len(prices) if prices else 0,
        avg_units=sum(units) / len(units) if units else 0,
        avg_cap_rate=sum(cap_rates) / len(cap_rates) if cap_rates else None,
        avg_price_per_unit=sum(price_per_units) / len(price_per_units) if price_per_units else 0,
        avg_price_per_sqft=sum(price_per_sqfts) / len(price_per_sqfts) if price_per_sqfts else 0,
        avg_price_per_acre=sum(price_per_acres) / len(price_per_acres) if price_per_acres else None,
        avg_noi=sum(nois) / len(nois) if nois else None,
        avg_unit_rents=avg_unit_rents
    )

def calculate_comparisons(om_data: Dict[str, Any], crexi_stats: CompStats, realtor_stats: CompStats) -> List[ComparisonResult]:
    """Calculate comparisons between OM data and market comps"""
    comparisons = []
    
    # Price per unit comparison
    if crexi_stats.avg_price_per_unit > 0:
        deviation = ((om_data['price_per_unit'] - crexi_stats.avg_price_per_unit) / crexi_stats.avg_price_per_unit) * 100
        comparisons.append(ComparisonResult(
            metric_name="Price per Unit",
            om_value=om_data['price_per_unit'],
            market_value=crexi_stats.avg_price_per_unit,
            deviation_percent=deviation,
            deviation_comment=f"{'+' if deviation > 0 else ''}{deviation:.1f}% vs CREXi market"
        ))
    
    # Price per sqft comparison
    if crexi_stats.avg_price_per_sqft > 0:
        deviation = ((om_data['price_per_sqft'] - crexi_stats.avg_price_per_sqft) / crexi_stats.avg_price_per_sqft) * 100
        comparisons.append(ComparisonResult(
            metric_name="Price per SqFt",
            om_value=om_data['price_per_sqft'],
            market_value=crexi_stats.avg_price_per_sqft,
            deviation_percent=deviation,
            deviation_comment=f"{'+' if deviation > 0 else ''}{deviation:.1f}% vs CREXi market"
        ))
    
    # Cap rate comparison
    if crexi_stats.avg_cap_rate and om_data['cap_rate']:
        deviation = om_data['cap_rate'] - crexi_stats.avg_cap_rate
        comparisons.append(ComparisonResult(
            metric_name="Cap Rate",
            om_value=om_data['cap_rate'],
            market_value=crexi_stats.avg_cap_rate,
            deviation_percent=deviation,
            deviation_comment=f"{'+' if deviation > 0 else ''}{deviation:.1f}pp vs CREXi market"
        ))
    
    # Unit rent comparisons
    if realtor_stats.avg_unit_rents:
        if om_data['unit_rents']['one_bed'] and realtor_stats.avg_unit_rents.one_bed:
            deviation = ((om_data['unit_rents']['one_bed'] - realtor_stats.avg_unit_rents.one_bed) / realtor_stats.avg_unit_rents.one_bed) * 100
            comparisons.append(ComparisonResult(
                metric_name="1BR Rent",
                om_value=om_data['unit_rents']['one_bed'],
                market_value=realtor_stats.avg_unit_rents.one_bed,
                deviation_percent=deviation,
                deviation_comment=f"{'+' if deviation > 0 else ''}{deviation:.1f}% vs Realtor market"
            ))
        
        if om_data['unit_rents']['two_bed'] and realtor_stats.avg_unit_rents.two_bed:
            deviation = ((om_data['unit_rents']['two_bed'] - realtor_stats.avg_unit_rents.two_bed) / realtor_stats.avg_unit_rents.two_bed) * 100
            comparisons.append(ComparisonResult(
                metric_name="2BR Rent",
                om_value=om_data['unit_rents']['two_bed'],
                market_value=realtor_stats.avg_unit_rents.two_bed,
                deviation_percent=deviation,
                deviation_comment=f"{'+' if deviation > 0 else ''}{deviation:.1f}% vs Realtor market"
            ))
    
    return comparisons

# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Hunting Party API is running", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.post("/analyze-om", response_model=AnalysisResponse)
async def analyze_om(
    filters: CompFilterRequest,
    om_csv: UploadFile = File(None)
):
    """
    Analyze an Offering Memorandum against market comps
    
    Accepts either CSV upload or uses mock data if no file provided.
    Pulls real market data from Supabase based on the property location.
    """
    try:
        # Process OM data
        if om_csv:
            # Read and parse CSV file
            csv_content = await om_csv.read()
            csv_text = csv_content.decode('utf-8')
            om_data = parse_csv_om_data(csv_text)
        else:
            # Use mock data if no CSV provided
            om_data = get_mock_om_data()
        
        # Extract geography from address for Supabase query
        geography = extract_geography_from_address(om_data['property_summary'].address)
        
        # Get market data from Supabase
        market_data = get_supabase_market_data(geography)
        crexi_comps = market_data["crexi"]
        realtor_comps = market_data["realtor"]
        
        # Fallback to mock data if Supabase data is empty
        if not crexi_comps:
            crexi_comps = get_mock_crexi_comps()
        if not realtor_comps:
            realtor_comps = get_mock_realtor_comps()
        
        # Apply filters to comparable properties
        crexi_comps = apply_filters(crexi_comps, filters)
        realtor_comps = apply_filters(realtor_comps, filters)
        
        # Calculate statistics
        crexi_stats = calculate_comp_stats(crexi_comps)
        realtor_stats = calculate_comp_stats(realtor_comps)
        
        # Calculate comparisons
        om_metrics = {
            'price_per_unit': om_data['financials'].price_per_unit,
            'price_per_sqft': om_data['financials'].price_per_sqft,
            'cap_rate': om_data['financials'].cap_rate,
            'unit_rents': asdict(om_data['unit_rents'])
        }
        comparisons = calculate_comparisons(om_metrics, crexi_stats, realtor_stats)
        
        # Store OM data in Supabase for future reference
        if supabase and om_csv:
            store_om_data_in_supabase(om_data)
        
        # Return response
        return AnalysisResponse(
            property_summary=asdict(om_data['property_summary']),
            unit_rents=asdict(om_data['unit_rents']),
            financials=asdict(om_data['financials']),
            vacancy_egi=asdict(om_data['vacancy_egi']),
            crexi_comps=asdict(crexi_stats),
            realtor_comps=asdict(realtor_stats),
            comparisons=[asdict(comp) for comp in comparisons],
            raw_crexi_comps=[asdict(comp) for comp in crexi_comps],
            raw_realtor_comps=[asdict(comp) for comp in realtor_comps]
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.get("/comps/crexi")
async def get_crexi_comps():
    """Get CREXi comparable properties"""
    try:
        comps = get_mock_crexi_comps()
        stats = calculate_comp_stats(comps)
        return {
            "comps": [asdict(comp) for comp in comps],
            "stats": asdict(stats)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get CREXi comps: {str(e)}")

@app.get("/comps/realtor")
async def get_realtor_comps():
    """Get Realtor.com comparable properties"""
    try:
        comps = get_mock_realtor_comps()
        stats = calculate_comp_stats(comps)
        return {
            "comps": [asdict(comp) for comp in comps],
            "stats": asdict(stats)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get Realtor comps: {str(e)}")

@app.get("/metrics/property-summary")
async def get_property_summary():
    """Get property summary from OM"""
    try:
        om_data = get_mock_om_data()
        return asdict(om_data['property_summary'])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get property summary: {str(e)}")

@app.get("/metrics/unit-rents")
async def get_unit_rents():
    """Get unit rent data from OM"""
    try:
        om_data = get_mock_om_data()
        return asdict(om_data['unit_rents'])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get unit rents: {str(e)}")

@app.get("/metrics/financials")
async def get_financials():
    """Get financial metrics from OM"""
    try:
        om_data = get_mock_om_data()
        return asdict(om_data['financials'])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get financials: {str(e)}")

@app.get("/csv-template")
async def get_csv_template():
    """Get CSV template for OM data upload"""
    template = {
        "columns": [
            "address",
            "opportunity_zone",
            "cap_rate",
            "rentable_sqft",
            "avg_sqft_per_unit",
            "asking_price",
            "lot_size_acres",
            "total_units",
            "one_bed_rent",
            "two_bed_rent",
            "three_bed_rent",
            "four_bed_rent",
            "noi",
            "price_per_sqft",
            "price_per_unit",
            "price_per_acre",
            "vacancy_rate",
            "gross_potential_rent"
        ],
        "example": {
            "address": "123 Main St, Tampa, FL",
            "opportunity_zone": "No",
            "cap_rate": "5.4",
            "rentable_sqft": "108750",
            "avg_sqft_per_unit": "920",
            "asking_price": "34250000",
            "lot_size_acres": "1.85",
            "total_units": "120",
            "one_bed_rent": "1650",
            "two_bed_rent": "2100",
            "three_bed_rent": "2450",
            "four_bed_rent": "2800",
            "noi": "1890000",
            "price_per_sqft": "315",
            "price_per_unit": "285417",
            "price_per_acre": "18513514",
            "vacancy_rate": "0.06",
            "gross_potential_rent": "2300000"
        }
    }
    return template

@app.get("/market-data/{geography}")
async def get_market_data(geography: str):
    """Get market data for a specific geography from Supabase"""
    try:
        market_data = get_supabase_market_data(geography)
        return {
            "geography": geography,
            "crexi_comps": [asdict(comp) for comp in market_data["crexi"]],
            "realtor_comps": [asdict(comp) for comp in market_data["realtor"]],
            "crexi_stats": asdict(calculate_comp_stats(market_data["crexi"])),
            "realtor_stats": asdict(calculate_comp_stats(market_data["realtor"]))
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get market data: {str(e)}")

# ============================================================================
# MAIN APPLICATION
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("APP_PORT", 3000))
    host = os.getenv("APP_HOST", "localhost")
    uvicorn.run("main:app", host=host, port=port, reload=True)