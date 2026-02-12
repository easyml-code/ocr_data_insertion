"""
Database models for GRN and PO tables
"""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date, datetime
from uuid import UUID, uuid4


class GRNHeader(BaseModel):
    """Model for s_grn_header table"""
    id: UUID = Field(default_factory=uuid4)
    is_deleted: bool = False
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[UUID] = None
    updated_by: Optional[UUID] = None
    s_effective_from: date
    s_effective_to: Optional[date] = None
    s_external_system: Optional[str] = None
    s_external_system_id: Optional[str] = None
    s_gl_account_ref: UUID
    s_grn_date: date
    s_grn_id: str
    s_grn_number: str
    s_grn_status: str
    s_legal_entity_site_ref: UUID
    s_po_line_ref: UUID
    s_qc_status: str
    s_supplier_site_ref: UUID
    s_total_received_amount: float
    s_total_received_qty: float
    s_total_received_weight: Optional[float] = None
    s_transport_mode: Optional[str] = None
    s_weight_uom_id: str


class GRNLine(BaseModel):
    """Model for s_grn_line table"""
    id: UUID = Field(default_factory=uuid4)
    is_deleted: bool = False
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[UUID] = None
    updated_by: Optional[UUID] = None
    s_grn_line_id: str
    s_item_description: str
    s_drawing_number: Optional[str] = None
    s_drawing_revision: Optional[str] = None
    s_uom_id: str
    s_received_qty: float
    s_unit_price: Optional[float] = None
    s_total_received_amount: Optional[float] = None
    s_accepted_qty: Optional[float] = None
    s_rejected_qty: Optional[float] = None
    s_rejection_reason: Optional[str] = None
    s_received_weight: Optional[float] = None
    s_weight_uom: str
    s_qc_required_flag: bool = False
    s_qc_result: str
    s_grn_line_status: str
    s_batch_number: Optional[str] = None
    s_manufacture_date: Optional[date] = None
    s_expiry_date: Optional[date] = None
    s_compliance_verified_flag: bool = False
    s_grn_ref: UUID
    s_item_ref: UUID
    s_effective_from: date
    s_effective_to: Optional[date] = None
    s_external_system_id: Optional[str] = None
    s_external_system: Optional[str] = None


class POHeader(BaseModel):
    """Model for s_po_header table"""
    id: UUID = Field(default_factory=uuid4)
    is_deleted: bool = False
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[UUID] = None
    updated_by: Optional[UUID] = None
    s_approved_by: Optional[str] = None
    s_cost_center_ref: UUID
    s_created_by: str
    s_currency_id: str
    s_effective_from: date
    s_effective_to: Optional[date] = None
    s_external_system: Optional[str] = None
    s_external_system_id: Optional[str] = None
    s_freight_included_flag: bool = False
    s_incoterms: Optional[str] = None
    s_legal_entity_ref: UUID
    s_legal_entity_site_ref: UUID
    s_matching_type: str
    s_payment_terms: str
    s_plant_ref: UUID
    s_po_date: date
    s_po_id: str
    s_po_number: str
    s_po_status: str
    s_po_total_value: float
    s_po_type: str
    s_po_valid_from: Optional[date] = None
    s_po_valid_to: Optional[date] = None
    s_profit_center_ref: UUID
    s_project_id: Optional[str] = None
    s_project_ref: UUID
    s_supplier_ref: UUID
    s_supplier_site_ref: UUID
    s_tax_rate_ref: UUID


class POLine(BaseModel):
    """Model for s_po_line table"""
    id: UUID = Field(default_factory=uuid4)
    is_deleted: bool = False
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[UUID] = None
    updated_by: Optional[UUID] = None
    s_batch_required_flag: bool = False
    s_cas_number: Optional[str] = None
    s_chemical_grade: Optional[str] = None
    s_closed_quantity: Optional[float] = None
    s_coa_required_flag: bool = False
    s_drawing_number: Optional[str] = None
    s_drawing_revision: Optional[str] = None
    s_effective_from: date
    s_effective_to: Optional[date] = None
    s_expected_delivery_date: Optional[date] = None
    s_expiry_required_flag: bool = False
    s_external_system: Optional[str] = None
    s_external_system_id: Optional[str] = None
    s_hsn_id: str
    s_item_ref: UUID
    s_line_amount: Optional[float] = None
    s_line_number: float
    s_line_status: str
    s_ordered_quantity: float
    s_po_header_ref: UUID
    s_po_line_id: str
    s_price_valid_from: Optional[date] = None
    s_price_valid_to: Optional[date] = None
    s_qc_required_flag: bool = False
    s_tolerance_pct: Optional[float] = None
    s_total_invoiced_qty: Optional[float] = None
    s_unit_price: float
    s_uom_id: str


class POCondition(BaseModel):
    """Model for s_po_condition table"""
    id: UUID = Field(default_factory=uuid4)
    is_deleted: bool = False
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: Optional[UUID] = None
    updated_by: Optional[UUID] = None
    s_calculation_basis: str
    s_condition_type: str
    s_effective_from: date
    s_effective_to: Optional[date] = None
    s_external_system: Optional[str] = None
    s_external_system_id: Optional[str] = None
    s_po_condition_id: str
    s_po_header_ref: UUID
    s_rate: float
    s_uom_id: str
