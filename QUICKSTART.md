# Quick Start Guide

## Prerequisites
- Python 3.9 or higher
- PostgreSQL database with required tables
- pip (Python package manager)

## Setup Steps

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
# Copy example env file
cp .env.example .env

# Edit .env with your database credentials
nano .env  # or use any text editor
```

Required environment variables:
```env
DB_HOST=your_database_host
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
```

### 3. Verify Database Schema
Ensure the following tables exist in your `tenant_data` schema:
- `s_grn_header`
- `s_grn_line`
- `s_po_header`
- `s_po_line`
- `s_po_condition`

### 4. Run the Application

#### Option A: API Server
```bash
python main.py
```
Access the API at: http://localhost:8000
Documentation: http://localhost:8000/docs

#### Option B: Direct Usage
```bash
python example_usage.py
```

## Testing the API

### Using Swagger UI
1. Open http://localhost:8000/docs
2. Click on `POST /api/v1/invoice/process`
3. Click "Try it out"
4. Use the sample data or load from `sample_invoice.json`
5. Click "Execute"

### Using curl
```bash
curl -X POST "http://localhost:8000/api/v1/invoice/process" \
  -H "Content-Type: application/json" \
  -d @sample_invoice.json
```

### Using Python requests
```python
import requests
import json

with open('sample_invoice.json', 'r') as f:
    invoice_data = json.load(f)

response = requests.post(
    'http://localhost:8000/api/v1/invoice/process',
    json=invoice_data
)

print(response.json())
```

## Common Issues

### Database Connection Error
- Verify PostgreSQL is running
- Check credentials in `.env`
- Ensure database and schema exist

### Import Errors
- Activate virtual environment if using one
- Run `pip install -r requirements.txt` again

### Validation Errors
- Check OCR data format matches expected structure
- Review sample_invoice.json for reference
- Check API documentation for field requirements

## Next Steps

1. Review the full README.md for detailed documentation
2. Check example_usage.py for programmatic usage examples
3. Customize mapping logic in services/mapper.py if needed
4. Implement actual reference resolution in utils/helpers.py
5. Add authentication/authorization as needed

## Project Structure
```
invoice_automation/
├── api/              # FastAPI endpoints
├── database/         # Database client
├── logs/             # Logging setup
├── models/           # Pydantic models
├── services/         # Business logic
├── utils/            # Helper functions
├── main.py           # FastAPI app
├── example_usage.py  # Usage examples
└── README.md         # Full documentation
```

## Support
- Full documentation: README.md
- API documentation: http://localhost:8000/docs
- Example usage: example_usage.py
