# Invoice Automation System

An automated system for processing OCR-extracted invoice data and inserting it into PostgreSQL database tables for GRN (Goods Receipt Note) and PO (Purchase Order) management.

## Features

- **OCR Data Processing**: Validates and transforms OCR-extracted invoice data
- **Automatic ID Generation**: Generates GRN numbers, PO IDs, and line IDs automatically
- **Database Integration**: Inserts data into PostgreSQL with proper relationships
- **Batch Processing**: Support for processing multiple invoices in a single operation
- **RESTful API**: FastAPI-based endpoints for easy integration
- **Type Safety**: Full Pydantic validation for data integrity
- **Logging**: Comprehensive logging for debugging and monitoring

## Architecture

```
invoice_automation/
├── api/
│   ├── __init__.py
│   └── endpoints.py          # FastAPI endpoints
├── database/
│   ├── __init__.py
│   └── client.py             # PostgreSQL connection
├── logs/
│   ├── __init__.py
│   └── log.py                # Logging configuration
├── models/
│   ├── __init__.py
│   ├── ocr_input.py          # OCR input validation models
│   └── db_models.py          # Database table models
├── services/
│   ├── __init__.py
│   ├── mapper.py             # OCR to DB mapping logic
│   ├── database.py           # Database operations
│   └── processor.py          # Main processing orchestration
├── utils/
│   ├── __init__.py
│   └── helpers.py            # ID generation & data transformation
├── config.py                 # Configuration settings
├── main.py                   # FastAPI application
├── example_usage.py          # Usage examples
├── requirements.txt          # Python dependencies
└── .env.example              # Environment variables template
```

## Installation

1. **Clone or copy the project structure**

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Configure environment variables**:
```bash
cp .env.example .env
# Edit .env with your database credentials
```

4. **Database Setup**:
Ensure PostgreSQL is running and the following tables exist in the `tenant_data` schema:
- `s_grn_header`
- `s_grn_line`
- `s_po_header`
- `s_po_line`
- `s_po_condition`

## Configuration

Edit `.env` file with your settings:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=invoice_db
DB_USER=postgres
DB_PASSWORD=your_password

# Authentication
LOGIN_URL=https://auth.example.com/login

# Application Settings
DEBUG=False
LOG_LEVEL=INFO
```

## Usage

### 1. Running the API Server

```bash
python main.py
```

The API will be available at:
- Main API: `http://localhost:8000`
- Documentation: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

### 2. API Endpoints

#### Process Single Invoice
```bash
POST /api/v1/invoice/process
```

**Request Body**:
```json
{
  "dynamic": [
    {
      "Invoice Lines/Description": "Dell UltraSharp Monitor",
      "Quantity": "1",
      "Line Amount": "17999.00",
      "Unit Price": "17999.00",
      "HSN Number": "852851",
      "igst_rate": "18",
      "PO Number": "9500877232"
    }
  ],
  "static": {
    "Invoice Date": ["15-Aug-2025"],
    "Invoice Currency": ["INR"],
    "Total Invoice Amount": ["21238.82"],
    "Supplier Name": ["Orion Corp"],
    "Invoice No": ["2024-29/09/007"]
  }
}
```

**Response**:
```json
{
  "status": "success",
  "message": "Invoice processed successfully",
  "grn_number": "GRN-20250815-A3F2B",
  "grn_id": "GRNID-12345678",
  "po_number": "9500877232",
  "details": {
    "grn_header_id": "uuid-here",
    "grn_lines_inserted": 1,
    "po_header_id": "uuid-here",
    "po_lines_inserted": 1,
    "po_conditions_inserted": 1
  },
  "errors": []
}
```

#### Process Batch Invoices
```bash
POST /api/v1/invoice/process/batch
```

**Request Body**:
```json
{
  "invoices": [
    {
      "dynamic": [...],
      "static": {...}
    },
    {
      "dynamic": [...],
      "static": {...}
    }
  ]
}
```

#### Health Check
```bash
GET /api/v1/invoice/health
```

### 3. Programmatic Usage

```python
import asyncio
from services.processor import InvoiceProcessor
from database.client import run_query

async def process_invoice():
    # OCR data
    ocr_data = {
        "dynamic": [...],
        "static": {...}
    }
    
    # Create processor
    processor = InvoiceProcessor(run_query)
    
    # Process invoice
    result = await processor.process_invoice(ocr_data)
    
    print(f"GRN Number: {result['grn_number']}")
    return result

# Run
asyncio.run(process_invoice())
```

### 4. Running Examples

```bash
python example_usage.py
```

## Data Flow

1. **OCR Input** → Validated using Pydantic models
2. **Mapping** → OCR data mapped to database models with auto-generated IDs
3. **Reference Resolution** → Resolve foreign key references (suppliers, items, etc.)
4. **Database Insertion** → Insert PO and GRN records with proper relationships

## Auto-Generated Fields

The system automatically generates:

- **GRN Number**: `GRN-YYYYMMDD-XXXXX`
- **GRN ID**: `GRNID-XXXXXXXX`
- **GRN Line ID**: `GRNLN-XXXXX-LLLL`
- **PO ID**: `POID-XXXXXXXX`
- **PO Line ID**: `POLN-XXXXX-LLLL`
- **PO Condition ID**: `POCOND-XXXXXXXX`
- **UUIDs**: For all primary and foreign keys
- **Timestamps**: Creation and update times
- **Line Numbers**: Sequential numbering for lines

## Database Schema

The system works with these tables in the `tenant_data` schema:

### s_grn_header
- Stores GRN header information
- Links to PO lines and supplier sites
- Tracks total quantities and amounts

### s_grn_line
- Individual GRN line items
- References items and GRN header
- Tracks received, accepted, and rejected quantities

### s_po_header
- Purchase Order header
- Links to suppliers, legal entities, and sites
- Contains payment terms and delivery information

### s_po_line
- Individual PO line items
- References items and PO header
- Tracks ordered quantities and prices

### s_po_condition
- Tax and other conditions on PO
- IGST, CGST, SGST rates
- Links to PO header

## Customization

### Adding New Reference Resolvers

Edit `utils/helpers.py`:

```python
class ReferenceResolver:
    def resolve_custom_ref(self, custom_field: str) -> UUID:
        # Add database query logic
        query = f"SELECT id FROM table WHERE field = '{custom_field}'"
        result = await run_query(query)
        return result[0]['id'] if result else uuid4()
```

### Modifying Mapping Logic

Edit `services/mapper.py`:

```python
def _create_grn_header(self, ...):
    # Add custom field mapping
    return GRNHeader(
        s_custom_field=self.transformer.extract_first(static.custom_field),
        ...
    )
```

### Adding Validation Rules

Edit `models/ocr_input.py`:

```python
from pydantic import validator

class InvoiceLine(BaseModel):
    quantity: str
    
    @validator('quantity')
    def validate_quantity(cls, v):
        if float(v) <= 0:
            raise ValueError('Quantity must be positive')
        return v
```

## Error Handling

The system provides detailed error messages:

- **Validation Errors**: Invalid OCR data format
- **Database Errors**: Connection or query failures
- **Mapping Errors**: Missing required fields

All errors are logged and returned in the API response.

## Logging

Logs include:
- Request processing
- Data validation
- Database operations
- Error traces

Configure log level in `.env`:
```env
LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR
```

## Testing

### Manual API Testing

Use the interactive Swagger UI at `http://localhost:8000/docs`

### Testing with curl

```bash
curl -X POST "http://localhost:8000/api/v1/invoice/process" \
  -H "Content-Type: application/json" \
  -d @sample_invoice.json
```

## Production Deployment

1. **Set DEBUG=False** in `.env`
2. **Configure proper CORS** in `main.py`
3. **Use production database** credentials
4. **Enable SSL** for database connections
5. **Set up proper authentication** (currently placeholder)
6. **Configure logging** to file or monitoring service
7. **Use production WSGI server** (gunicorn/uvicorn workers)

```bash
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker
```

## Reference Resolution

Currently using **placeholder mode** which generates UUIDs for missing references. 

For production:
1. Implement actual database lookups in `ReferenceResolver` class
2. Query master data tables for suppliers, items, legal entities, etc.
3. Handle cases where references don't exist

Example production implementation:

```python
async def resolve_supplier_ref(self, supplier_gstn: str) -> UUID:
    query = f"""
        SELECT id FROM tenant_data.m_supplier 
        WHERE s_gstn = '{supplier_gstn}'
    """
    result = await run_query(query)
    
    if not result:
        raise ValueError(f"Supplier not found: {supplier_gstn}")
    
    return result[0]['id']
```

## Troubleshooting

### Database Connection Issues
- Check PostgreSQL is running
- Verify credentials in `.env`
- Ensure database and schema exist
- Check network/firewall settings

### OCR Data Validation Errors
- Verify JSON structure matches expected format
- Check required fields are present
- Validate date formats
- Ensure numeric fields are strings or numbers

### Missing References
- Currently uses placeholder UUIDs
- Implement actual reference resolution for production
- Pre-populate master data tables

## Contributing

1. Follow existing code structure
2. Add type hints for all functions
3. Update models when schema changes
4. Add logging for new operations
5. Update documentation

## License

[Your License Here]

## Support

For issues or questions:
- Check logs for detailed error messages
- Review API documentation at `/docs`
- Examine example usage in `example_usage.py`
