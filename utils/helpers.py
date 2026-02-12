"""
Utility functions for ID generation and data transformation
"""
from datetime import datetime, date
from uuid import UUID, uuid4
import hashlib
from typing import Optional, Any
from decimal import Decimal


class IDGenerator:
    """Generate unique IDs for various entities"""
    
    @staticmethod
    def generate_grn_number(invoice_no: str, timestamp: Optional[datetime] = None) -> str:
        """
        Generate unique GRN number based on invoice number and timestamp
        Format: GRN-YYYYMMDD-XXXXX
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        date_part = timestamp.strftime("%Y%m%d")
        
        # Create a hash from invoice_no to ensure uniqueness
        hash_part = hashlib.md5(invoice_no.encode()).hexdigest()[:5].upper()
        
        return f"GRN-{date_part}-{hash_part}"
    
    @staticmethod
    def generate_grn_id(grn_number: str) -> str:
        """
        Generate GRN ID from GRN number
        Format: GRNID-XXXXX
        """
        hash_part = hashlib.md5(grn_number.encode()).hexdigest()[:8].upper()
        return f"GRNID-{hash_part}"
    
    @staticmethod
    def generate_grn_line_id(grn_id: str, line_number: int) -> str:
        """
        Generate GRN line ID
        Format: GRNLN-XXXXX-LLLL
        """
        hash_part = hashlib.md5(grn_id.encode()).hexdigest()[:5].upper()
        return f"GRNLN-{hash_part}-{line_number:04d}"
    
    @staticmethod
    def generate_po_id(po_number: str) -> str:
        """
        Generate PO ID from PO number
        Format: POID-XXXXX
        """
        hash_part = hashlib.md5(po_number.encode()).hexdigest()[:8].upper()
        return f"POID-{hash_part}"
    
    @staticmethod
    def generate_po_line_id(po_id: str, line_number: int) -> str:
        """
        Generate PO line ID
        Format: POLN-XXXXX-LLLL
        """
        hash_part = hashlib.md5(po_id.encode()).hexdigest()[:5].upper()
        return f"POLN-{hash_part}-{line_number:04d}"
    
    @staticmethod
    def generate_po_condition_id(po_id: str, condition_type: str) -> str:
        """
        Generate PO condition ID
        Format: POCOND-XXXXX
        """
        combined = f"{po_id}-{condition_type}"
        hash_part = hashlib.md5(combined.encode()).hexdigest()[:8].upper()
        return f"POCOND-{hash_part}"


class DataTransformer:
    """Transform and normalize OCR data"""
    
    @staticmethod
    def parse_date(date_str: Optional[str], date_format: str = "%d-%b-%Y") -> Optional[date]:
        """
        Parse date string to date object
        Supports multiple formats: DD-MMM-YYYY, DD/MM/YYYY, YYYY-MM-DD
        """
        if not date_str:
            return None
        
        formats = [
            "%d-%b-%Y",  # 15-Aug-2025
            "%d/%m/%Y",  # 15/08/2025
            "%Y-%m-%d",  # 2025-08-15
            "%d-%m-%Y",  # 15-08-2025
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        return None
    
    @staticmethod
    def safe_float(value: Any, default: float = 0.0) -> float:
        """Safely convert value to float"""
        if value is None or value == "":
            return default
        
        try:
            # Remove commas and convert
            if isinstance(value, str):
                value = value.replace(",", "")
            return float(value)
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def safe_decimal(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
        """Safely convert value to Decimal"""
        if value is None or value == "":
            return default
        
        try:
            if isinstance(value, str):
                value = value.replace(",", "")
            return Decimal(str(value))
        except (ValueError, TypeError):
            return default
    
    @staticmethod
    def extract_first(value: Any) -> Optional[str]:
        """Extract first element from list or return string"""
        if isinstance(value, list) and len(value) > 0:
            return str(value[0]) if value[0] is not None else None
        return str(value) if value else None
    
    @staticmethod
    def clean_string(value: Optional[str]) -> Optional[str]:
        """Clean and normalize string"""
        if not value:
            return None
        return str(value).strip()
    
    @staticmethod
    def extract_tax_rate(tax_str: Optional[str]) -> float:
        """
        Extract numeric tax rate from string like '18%'
        """
        if not tax_str:
            return 0.0
        
        # Remove % and any non-numeric characters except decimal point
        cleaned = ''.join(c for c in str(tax_str) if c.isdigit() or c == '.')
        return DataTransformer.safe_float(cleaned, 0.0)
    
    @staticmethod
    def generate_placeholder_uuid() -> UUID:
        """Generate a placeholder UUID for missing references"""
        return uuid4()


class ReferenceResolver:
    """Resolve references to existing entities in database"""
    
    def __init__(self, placeholder_mode: bool = True):
        """
        Initialize resolver
        
        Args:
            placeholder_mode: If True, generate placeholder UUIDs for missing refs.
                            If False, raise errors for missing refs.
        """
        self.placeholder_mode = placeholder_mode
    
    def resolve_supplier_ref(self, supplier_name: Optional[str], supplier_gstn: Optional[str]) -> UUID:
        """
        Resolve supplier reference
        In production, this would query the database for the supplier UUID
        """
        # TODO: Query database for supplier by name/GSTN
        # For now, generate placeholder
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_item_ref(self, item_description: str, hsn_code: Optional[str] = None) -> UUID:
        """
        Resolve item reference
        In production, this would query the database for the item UUID
        """
        # TODO: Query database for item by description/HSN
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_legal_entity_ref(self, gstn: Optional[str] = None) -> UUID:
        """
        Resolve legal entity reference
        In production, this would query the database for the legal entity UUID
        """
        # TODO: Query database for legal entity by GSTN
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_site_ref(self, address: Optional[str], gstn: Optional[str] = None) -> UUID:
        """
        Resolve site reference
        In production, this would query the database for the site UUID
        """
        # TODO: Query database for site by address/GSTN
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_cost_center_ref(self) -> UUID:
        """Resolve cost center reference"""
        # TODO: Query database or use default cost center
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_profit_center_ref(self) -> UUID:
        """Resolve profit center reference"""
        # TODO: Query database or use default profit center
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_project_ref(self, project_id: Optional[str] = None) -> UUID:
        """Resolve project reference"""
        # TODO: Query database for project
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_plant_ref(self, location: Optional[str] = None) -> UUID:
        """Resolve plant reference"""
        # TODO: Query database for plant
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_gl_account_ref(self) -> UUID:
        """Resolve GL account reference"""
        # TODO: Query database for GL account
        return DataTransformer.generate_placeholder_uuid()
    
    def resolve_tax_rate_ref(self, tax_rate: float) -> UUID:
        """Resolve tax rate reference"""
        # TODO: Query database for tax rate
        return DataTransformer.generate_placeholder_uuid()
