# Backend API

FastAPI backend for real estate investment analysis.

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Configure `.env` with Supabase credentials
3. Run: `python api/main.py`

## Key Endpoints

- `POST /analyze-om` - Analyze OM (CSV upload + filters)
- `GET /csv-template` - Get CSV format
- `GET /market-data/{geography}` - Get market data

## CSV Format

Required columns: `address`, `opportunity_zone`, `cap_rate`, `rentable_sqft`, `avg_sqft_per_unit`, `asking_price`, `lot_size_acres`, `total_units`, `one_bed_rent`, `two_bed_rent`, `three_bed_rent`, `four_bed_rent`, `noi`, `price_per_sqft`, `price_per_unit`, `price_per_acre`, `vacancy_rate`, `gross_potential_rent`

## Usage

```bash
# Upload CSV and analyze
curl -X POST "http://localhost:3000/analyze-om" \
  -F "om_csv=@property_data.csv" \
  -F 'filters={"price_min": 30000000, "units_min": 100}'

# Get CSV template
curl "http://localhost:3000/csv-template"
```

API docs: http://localhost:3000/docs
