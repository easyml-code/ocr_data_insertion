"""
Pydantic models for OCR input validation
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any


class InvoiceLine(BaseModel):
    """Model for individual invoice line items"""
    description: Optional[str] = Field(None, alias="Invoice Lines/Description")
    quantity: Optional[str] = Field(None, alias="Quantity")
    line_amount: Optional[str] = Field(None, alias="Line Amount")
    unit_price: Optional[str] = Field(None, alias="Unit Price")
    hsn_number: Optional[str] = Field(None, alias="HSN Number")
    igst_rate: Optional[str] = None
    sgst_rate: Optional[str] = None
    cgst_rate: Optional[str] = None
    utgst_rate: Optional[str] = None
    po_number: Optional[str] = Field(None, alias="PO Number")
    line_no: Optional[str] = None
    unit: Optional[str] = None

    class Config:
        populate_by_name = True


class StaticData(BaseModel):
    """Model for static invoice data"""
    invoice_date: Optional[List[str]] = Field(None, alias="Invoice Date")
    invoice_currency: Optional[List[str]] = Field(None, alias="Invoice Currency")
    supplier_city: Optional[List[str]] = None
    supplier_state: Optional[List[str]] = None
    total_invoice_amount: Optional[List[str]] = Field(None, alias="Total Invoice Amount")
    invoice_tax_amount: Optional[List[str]] = Field(None, alias="Invoice Tax Amount")
    subtotal: Optional[List[str]] = None
    delivery_location: Optional[List[str]] = None
    invoice_information: Optional[List[str]] = Field(None, alias="Invoice information")
    account_number: Optional[List[str]] = None
    bill_to_address: Optional[List[str]] = None
    ship_to_address: Optional[List[str]] = None
    shipping_amount: Optional[List[str]] = None
    hsn_code: Optional[List[str]] = None
    supplier_gstn: Optional[List[str]] = Field(None, alias="Supplier GSTN")
    location_gstn: Optional[List[str]] = Field(None, alias="Location GSTN")
    supplier_name: Optional[List[str]] = Field(None, alias="Supplier Name")
    irn: Optional[List[str]] = None
    invoice_no: Optional[List[str]] = Field(None, alias="Invoice No")
    cgst: Optional[List[str]] = Field(None, alias="CGST")
    sgst: Optional[List[str]] = Field(None, alias="SGST")
    igst: Optional[List[str]] = Field(None, alias="IGST")
    po_number: Optional[List[str]] = Field(None, alias="PO Number")
    supplier_address: Optional[List[str]] = None
    consumer_number: Optional[List[str]] = None
    customer_address: Optional[List[str]] = None
    file_name: Optional[List[str]] = None
    gst_number: Optional[List[str]] = None
    due_date: Optional[List[str]] = None
    applicable_tax: Optional[List[str]] = Field(None, alias="Applicable Tax")
    currency: Optional[List[str]] = None

    class Config:
        populate_by_name = True


class OCRInput(BaseModel):
    """Main OCR input model"""
    dynamic: List[InvoiceLine]
    static: StaticData
