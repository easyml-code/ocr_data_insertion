"""
Utility functions for ID generation and data transformation
Following SAP and Oracle ERP standards for realistic data generation
"""
from datetime import datetime, date, timedelta
from uuid import UUID, uuid4
import hashlib
import random
from typing import Optional, Any
from decimal import Decimal


class IDGenerator:
    """
    Generate unique IDs following enterprise ERP patterns
    Based on SAP MM/SD and Oracle Procurement Cloud standards
    NOTE: PO numbers are NEVER generated - they come from OCR input
    """
    
    # SAP-style number ranges
    _grn_counter = random.randint(5000000000, 5099999999)
    _invoice_counter = random.randint(1900000000, 1999999999)
    
    @classmethod
    def _increment_counter(cls, counter_name: str) -> int:
        """Increment and return counter value"""
        current = getattr(cls, counter_name)
        setattr(cls, counter_name, current + 1)
        return current
    
    @staticmethod
    def generate_po_id(po_number: str) -> str:
        """
        Generate PO ID - internal system identifier from existing PO number
        Oracle Cloud format: Alphanumeric, max 36 chars
        """
        # Use timestamp + hash for uniqueness
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        hash_suffix = hashlib.sha256(po_number.encode()).hexdigest()[:6].upper()
        return f"{po_number}-{hash_suffix}"
    
    @staticmethod
    def generate_grn_number(
        plant_code: str = "1000",
        movement_type: str = "101",
        fiscal_year: Optional[int] = None
    ) -> str:
        """
        Generate realistic GRN number following SAP Material Document pattern
        SAP Format: Material Doc = 10-digit numeric (e.g., 5000123456)
        Format: Sequential 10-digit number
        """
        if fiscal_year is None:
            fiscal_year = datetime.now().year
        
        # Get sequential number
        seq = IDGenerator._increment_counter('_grn_counter')
        
        # Return 10-digit format like SAP Material Document
        return str(seq)
    
    @staticmethod
    def generate_grn_id(grn_number: str) -> str:
        """
        Generate GRN ID - internal unique identifier
        Format: GRN number + fiscal year + checksum
        """
        fiscal_year = datetime.now().year % 100  # Last 2 digits
        # Add checksum for data integrity (simplified Luhn-like)
        checksum = sum(int(d) for d in grn_number) % 10
        return f"{grn_number}{fiscal_year}{checksum}"
    
    @staticmethod
    def generate_grn_line_id(grn_id: str, line_number: int) -> str:
        """
        Generate GRN line ID
        SAP Format: Material Doc + Line (e.g., 5000123456-0010)
        Line numbers in SAP are 4-digit with leading zeros
        """
        return f"{grn_id}-{line_number:04d}"
    
    @staticmethod
    def generate_po_line_id(po_id: str, line_number: int) -> str:
        """
        Generate PO line ID
        SAP Format: PO Number + Line (e.g., 4500123456-00010)
        Line numbers padded to 5 digits in SAP
        """
        # Extract just the numeric part if po_id has suffix
        po_base = po_id.split('-')[0]
        return f"{po_base}-{line_number:05d}"
    
    @staticmethod
    def generate_po_condition_id(po_id: str, condition_type: str) -> str:
        """
        Generate PO condition ID
        SAP Format: PO + Condition Type + Counter
        Condition types: MWST (VAT), NAVS (Non-deductible), BASB (Cash Discount)
        """
        # Map common tax types to SAP condition types
        condition_map = {
            'IGST': 'JIGG',  # Indian GST Integrated
            'CGST': 'JICG',  # Indian GST Central
            'SGST': 'JISG',  # Indian GST State
            'VAT': 'MWST',
            'FREIGHT': 'FRA1',
            'DISCOUNT': 'SKTO'
        }
        
        sap_condition = condition_map.get(condition_type, condition_type)
        po_base = po_id.split('-')[0]
        
        # Generate unique suffix
        hash_suffix = hashlib.md5(f"{po_id}{condition_type}".encode()).hexdigest()[:4].upper()
        
        return f"{po_base}-{sap_condition}-{hash_suffix}"
    
    @staticmethod
    def generate_invoice_number(
        company_code: str = "1000",
        fiscal_year: Optional[int] = None,
        vendor_prefix: Optional[str] = None
    ) -> str:
        """
        Generate invoice reference number
        Oracle format: Mix of vendor reference and system-generated
        """
        if fiscal_year is None:
            fiscal_year = datetime.now().year
        
        seq = IDGenerator._increment_counter('_invoice_counter')
        
        # If vendor has their own numbering, preserve it
        if vendor_prefix:
            return f"{vendor_prefix}-{seq}"
        
        # Otherwise use fiscal year + sequence
        return f"{fiscal_year}{seq}"
    
    @staticmethod
    def generate_batch_number(
        material_code: Optional[str] = None,
        manufacture_date: Optional[date] = None
    ) -> str:
        """
        Generate batch number for materials
        Common formats:
        - YYYYMMDD-XXX (Date-based)
        - LOT-XXXXXXXX (Lot-based)
        - B-YYDDD-XXX (Julian date format)
        """
        if manufacture_date is None:
            manufacture_date = date.today()
        
        # Use Julian date format (common in pharma/food industry)
        year_2digit = manufacture_date.year % 100
        day_of_year = manufacture_date.timetuple().tm_yday
        
        # Add random suffix for uniqueness within same day
        suffix = random.randint(100, 999)
        
        return f"B{year_2digit:02d}{day_of_year:03d}{suffix}"


class DataTransformer:
    """Transform and normalize OCR data with validation"""
    
    @staticmethod
    def parse_date(date_str: Optional[str], date_format: str = "%d-%b-%Y") -> Optional[date]:
        """
        Parse date string to date object
        Supports multiple formats: DD-MMM-YYYY, DD/MM/YYYY, YYYY-MM-DD
        """
        if not date_str:
            return None
        
        # Clean the input
        date_str = str(date_str).strip()
        
        formats = [
            "%d-%b-%Y",  # 15-Aug-2025
            "%d/%m/%Y",  # 15/08/2025
            "%Y-%m-%d",  # 2025-08-15
            "%d-%m-%Y",  # 15-08-2025
            "%d.%m.%Y",  # 15.08.2025 (European)
            "%m/%d/%Y",  # 08/15/2025 (US)
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        # If all fail, log and return None
        return None
    
    @staticmethod
    def safe_float(value: Any, default: float = 0.0, precision: int = 2) -> float:
        """Safely convert value to float with precision control"""
        if value is None or value == "":
            return default
        
        try:
            # Remove commas, spaces, and currency symbols
            if isinstance(value, str):
                value = value.replace(",", "").replace(" ", "")
                # Remove common currency symbols
                value = value.replace("₹", "").replace("$", "").replace("€", "")
                value = value.strip()
            
            result = float(value)
            # Round to specified precision
            return round(result, precision)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def safe_decimal(value: Any, default: Optional[Decimal] = None, scale: int = 4) -> Optional[Decimal]:
        """
        Safely convert value to Decimal with scale control
        Scale matches database column precision (typically 4 for amounts, 2 for quantities)
        """
        if value is None or value == "":
            return default
        
        try:
            if isinstance(value, str):
                value = value.replace(",", "").replace(" ", "")
                value = value.replace("₹", "").replace("$", "").replace("€", "")
                value = value.strip()
            
            dec_value = Decimal(str(value))
            # Quantize to specified scale
            quantize_value = Decimal(10) ** -scale
            return dec_value.quantize(quantize_value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def extract_first(value: Any) -> Optional[str]:
        """Extract first element from list or return string"""
        if isinstance(value, list) and len(value) > 0:
            return str(value[0]).strip() if value[0] is not None else None
        return str(value).strip() if value else None
    
    @staticmethod
    def clean_string(value: Optional[str], max_length: Optional[int] = None) -> Optional[str]:
        """Clean and normalize string with length validation"""
        if not value:
            return None
        
        cleaned = str(value).strip()
        
        # Remove extra whitespace
        cleaned = ' '.join(cleaned.split())
        
        # Truncate if needed
        if max_length and len(cleaned) > max_length:
            cleaned = cleaned[:max_length]
        
        return cleaned if cleaned else None
    
    @staticmethod
    def extract_tax_rate(tax_str: Optional[str]) -> float:
        """
        Extract numeric tax rate from string like '18%' or '18.00'
        Returns rate as percentage (18.0 for 18%)
        """
        if not tax_str:
            return 0.0
        
        # Convert to string and clean
        tax_str = str(tax_str).strip()
        
        # Remove % and any non-numeric characters except decimal point
        cleaned = ''.join(c for c in tax_str if c.isdigit() or c == '.')
        
        if not cleaned:
            return 0.0
        
        rate = DataTransformer.safe_float(cleaned, 0.0)
        
        # If rate is > 1 and doesn't have %, it's already a percentage
        # If rate is <= 1, it might be decimal (0.18 for 18%)
        if 0 < rate <= 1:
            rate = rate * 100
        
        return round(rate, 2)
    
    @staticmethod
    def normalize_uom(uom: Optional[str]) -> str:
        """
        Normalize Unit of Measure to SAP/Oracle standard codes
        """
        if not uom:
            return "EA"  # Each (default)
        
        uom = uom.strip().upper()
        
        # Common UOM mappings
        uom_map = {
            'EACH': 'EA',
            'PIECE': 'EA',
            'PCS': 'EA',
            'PC': 'EA',
            'NOS': 'EA',
            'UNIT': 'EA',
            'KILOGRAM': 'KG',
            'KILOGRAMS': 'KG',
            'KGS': 'KG',
            'GRAM': 'G',
            'GRAMS': 'G',
            'LITER': 'L',
            'LITERS': 'L',
            'METER': 'M',
            'METERS': 'M',
            'BOX': 'BX',
            'CARTON': 'CT',
            'PALLET': 'PL',
        }
        
        return uom_map.get(uom, uom if len(uom) <= 3 else 'EA')
    
    @staticmethod
    def generate_placeholder_uuid() -> UUID:
        """Generate a placeholder UUID for missing references"""
        return uuid4()
    
    @staticmethod
    def calculate_expected_delivery_date(
        po_date: date,
        lead_time_days: int = 14
    ) -> date:
        """Calculate expected delivery date based on lead time"""
        return po_date + timedelta(days=lead_time_days)
    
    @staticmethod
    def extract_hsn_code(hsn: Optional[str]) -> str:
        """
        Extract and validate HSN code
        Indian HSN codes are 4, 6, or 8 digits
        """
        if not hsn:
            return "999999"  # Default/Unknown
        
        # Extract only digits
        hsn_digits = ''.join(c for c in str(hsn) if c.isdigit())
        
        if not hsn_digits:
            return "999999"
        
        # Ensure it's 6 digits (standard for India GST)
        if len(hsn_digits) < 6:
            hsn_digits = hsn_digits.ljust(6, '0')
        elif len(hsn_digits) > 8:
            hsn_digits = hsn_digits[:8]
        
        return hsn_digits


class ReferenceResolver:
    """
    Resolve references to existing entities in database
    In production, these would query actual master data tables
    """
    
    def __init__(self, placeholder_mode: bool = True):
        """
        Initialize resolver
        
        Args:
            placeholder_mode: If True, generate placeholder UUIDs for missing refs.
                            If False, raise errors for missing refs.
        """
        self.placeholder_mode = placeholder_mode
        # Cache for resolved references to maintain consistency within a session
        self._cache = {}
    
    def _get_or_create_ref(self, key: str, generator_func) -> UUID:
        """Get cached reference or create new one"""
        if key not in self._cache:
            self._cache[key] = generator_func()
        return self._cache[key]
    
    def resolve_supplier_ref(
        self, 
        supplier_name: Optional[str], 
        supplier_gstn: Optional[str]
    ) -> UUID:
        """
        Resolve supplier reference
        In production: Query SUPPLIER table by GSTN or name
        """
        cache_key = f"supplier_{supplier_gstn or supplier_name}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_item_ref(
        self, 
        item_description: str, 
        hsn_code: Optional[str] = None
    ) -> UUID:
        """
        Resolve item reference
        In production: Query ITEM table by description/HSN
        """
        cache_key = f"item_{hsn_code or item_description}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_legal_entity_ref(self, gstn: Optional[str] = None) -> UUID:
        """
        Resolve legal entity reference
        In production: Query LEGAL_ENTITY table by GSTN
        """
        cache_key = f"legal_entity_{gstn}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_site_ref(
        self, 
        address: Optional[str], 
        gstn: Optional[str] = None
    ) -> UUID:
        """
        Resolve site reference (Supplier Site or Legal Entity Site)
        In production: Query SITE tables by address/GSTN
        """
        cache_key = f"site_{gstn or address}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_cost_center_ref(self, cost_center_code: Optional[str] = None) -> UUID:
        """
        Resolve cost center reference
        In production: Query COST_CENTER table
        """
        cache_key = f"cost_center_{cost_center_code or 'default'}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_profit_center_ref(self, profit_center_code: Optional[str] = None) -> UUID:
        """
        Resolve profit center reference
        In production: Query PROFIT_CENTER table
        """
        cache_key = f"profit_center_{profit_center_code or 'default'}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_project_ref(self, project_id: Optional[str] = None) -> UUID:
        """
        Resolve project reference
        In production: Query PROJECT_WBS table
        """
        cache_key = f"project_{project_id or 'default'}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_plant_ref(self, plant_code: Optional[str] = None) -> UUID:
        """
        Resolve plant/warehouse reference
        In production: Query PLANT table
        """
        cache_key = f"plant_{plant_code or 'default'}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_gl_account_ref(self, account_code: Optional[str] = None) -> UUID:
        """
        Resolve GL account reference
        In production: Query GL_ACCOUNT table
        """
        cache_key = f"gl_account_{account_code or 'default'}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_tax_rate_ref(self, tax_rate: float, tax_type: str = "GST") -> UUID:
        """
        Resolve tax rate reference
        In production: Query TAX_RATE table by rate and type
        """
        cache_key = f"tax_rate_{tax_type}_{tax_rate}"
        return self._get_or_create_ref(cache_key, DataTransformer.generate_placeholder_uuid)
    
    def resolve_currency_id(self, currency_code: str = "INR") -> str:
        """
        Resolve currency ID
        Returns ISO currency code
        """
        # Validate currency code
        valid_currencies = ['INR', 'USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'SGD']
        return currency_code.upper() if currency_code.upper() in valid_currencies else 'INR'
    
    def clear_cache(self):
        """Clear reference cache - useful for batch processing"""
        self._cache.clear()