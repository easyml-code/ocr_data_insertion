"""
Service for mapping OCR data to database models
"""
from datetime import datetime, date
from typing import List, Tuple, Optional
from uuid import UUID

from models.ocr_input import OCRInput, InvoiceLine
from models.db_models import (
    GRNHeader, GRNLine, POHeader, POLine, POCondition
)
from utils.helpers import (
    IDGenerator, DataTransformer, ReferenceResolver
)
from logs.log import logger


class OCRMapper:
    """Maps OCR extracted data to database models"""
    
    def __init__(self):
        self.id_gen = IDGenerator()
        self.transformer = DataTransformer()
        self.resolver = ReferenceResolver(placeholder_mode=True)
    
    def map_to_database_models(
        self, 
        ocr_data: OCRInput
    ) -> Tuple[
        Optional[POHeader],
        List[POLine],
        List[POCondition],
        GRNHeader,
        List[GRNLine]
    ]:
        """
        Map OCR data to all required database models
        
        Returns:
            Tuple of (po_header, po_lines, po_conditions, grn_header, grn_lines)
        """
        static = ocr_data.static
        lines = ocr_data.dynamic
        
        # Extract common data
        invoice_no = self.transformer.extract_first(static.invoice_no)
        invoice_date = self.transformer.parse_date(
            self.transformer.extract_first(static.invoice_date)
        ) or date.today()
        po_number = self.transformer.extract_first(static.po_number)
        supplier_name = self.transformer.extract_first(static.supplier_name)
        supplier_gstn = self.transformer.extract_first(static.supplier_gstn)
        location_gstn = self.transformer.extract_first(static.location_gstn)
        currency = self.transformer.extract_first(static.invoice_currency) or "INR"
        
        # Generate IDs
        grn_number = self.id_gen.generate_grn_number(invoice_no or "UNKNOWN")
        grn_id = self.id_gen.generate_grn_id(grn_number)
        
        # Resolve references
        supplier_ref = self.resolver.resolve_supplier_ref(supplier_name, supplier_gstn)
        supplier_site_ref = self.resolver.resolve_site_ref(
            self.transformer.extract_first(static.supplier_address),
            supplier_gstn
        )
        legal_entity_ref = self.resolver.resolve_legal_entity_ref(location_gstn)
        legal_entity_site_ref = self.resolver.resolve_site_ref(
            self.transformer.extract_first(static.bill_to_address),
            location_gstn
        )
        
        # Create PO Header (if PO number exists)
        po_header = None
        if po_number:
            po_header = self._create_po_header(
                po_number=po_number,
                po_date=invoice_date,
                supplier_ref=supplier_ref,
                supplier_site_ref=supplier_site_ref,
                legal_entity_ref=legal_entity_ref,
                legal_entity_site_ref=legal_entity_site_ref,
                currency=currency,
                total_value=self.transformer.safe_float(
                    self.transformer.extract_first(static.total_invoice_amount)
                )
            )
        
        # Create PO Lines
        po_lines = []
        po_line_refs = {}  # Map line number to UUID
        
        if po_header:
            for idx, line in enumerate(lines, start=1):
                po_line = self._create_po_line(
                    po_header_ref=po_header.id,
                    line_number=idx,
                    line_data=line,
                    effective_from=invoice_date
                )
                po_lines.append(po_line)
                po_line_refs[idx] = po_line.id
        
        # Create PO Conditions (tax conditions)
        po_conditions = []
        if po_header:
            po_conditions = self._create_po_conditions(
                po_header_ref=po_header.id,
                igst=self.transformer.extract_first(static.igst),
                cgst=self.transformer.extract_first(static.cgst),
                sgst=self.transformer.extract_first(static.sgst),
                effective_from=invoice_date
            )
        
        # Calculate totals for GRN
        total_qty = sum(
            self.transformer.safe_float(line.quantity) 
            for line in lines
        )
        total_amount = self.transformer.safe_float(
            self.transformer.extract_first(static.subtotal)
        )
        
        # Create GRN Header
        # Use first PO line ref if available, otherwise generate placeholder
        first_po_line_ref = (
            po_line_refs.get(1) if po_line_refs 
            else self.resolver.resolve_project_ref()
        )
        
        grn_header = self._create_grn_header(
            grn_number=grn_number,
            grn_id=grn_id,
            grn_date=invoice_date,
            supplier_site_ref=supplier_site_ref,
            legal_entity_site_ref=legal_entity_site_ref,
            po_line_ref=first_po_line_ref,
            total_qty=total_qty,
            total_amount=total_amount,
            effective_from=invoice_date
        )
        
        # Create GRN Lines
        grn_lines = []
        for idx, line in enumerate(lines, start=1):
            grn_line = self._create_grn_line(
                grn_ref=grn_header.id,
                line_number=idx,
                line_data=line,
                grn_id=grn_id,
                effective_from=invoice_date
            )
            grn_lines.append(grn_line)
        
        logger.info(
            "Mapped OCR data: invoice_no=%s, grn_number=%s, po_lines=%d, grn_lines=%d",
            invoice_no, grn_number, len(po_lines), len(grn_lines)
        )
        
        return po_header, po_lines, po_conditions, grn_header, grn_lines
    
    def _create_po_header(
        self,
        po_number: str,
        po_date: date,
        supplier_ref: UUID,
        supplier_site_ref: UUID,
        legal_entity_ref: UUID,
        legal_entity_site_ref: UUID,
        currency: str,
        total_value: float
    ) -> POHeader:
        """Create PO Header model"""
        po_id = self.id_gen.generate_po_id(po_number)
        
        return POHeader(
            s_po_number=po_number,
            s_po_id=po_id,
            s_po_date=po_date,
            s_po_status="APPROVED",
            s_po_type="STANDARD",
            s_supplier_ref=supplier_ref,
            s_supplier_site_ref=supplier_site_ref,
            s_legal_entity_ref=legal_entity_ref,
            s_legal_entity_site_ref=legal_entity_site_ref,
            s_currency_id=currency,
            s_po_total_value=total_value,
            s_cost_center_ref=self.resolver.resolve_cost_center_ref(),
            s_profit_center_ref=self.resolver.resolve_profit_center_ref(),
            s_project_ref=self.resolver.resolve_project_ref(),
            s_plant_ref=self.resolver.resolve_plant_ref(),
            s_tax_rate_ref=self.resolver.resolve_tax_rate_ref(0.0),
            s_created_by="OCR_AUTOMATION",
            s_payment_terms="NET30",
            s_matching_type="THREE_WAY",
            s_effective_from=po_date,
            s_external_system="OCR_SYSTEM",
            s_external_system_id=f"OCR-{po_number}"
        )
    
    def _create_po_line(
        self,
        po_header_ref: UUID,
        line_number: int,
        line_data: InvoiceLine,
        effective_from: date
    ) -> POLine:
        """Create PO Line model"""
        po_line_id = self.id_gen.generate_po_line_id(
            str(po_header_ref), 
            line_number
        )
        
        quantity = self.transformer.safe_float(line_data.quantity)
        unit_price = self.transformer.safe_float(line_data.unit_price)
        line_amount = self.transformer.safe_float(line_data.line_amount)
        
        # Resolve item reference
        item_ref = self.resolver.resolve_item_ref(
            line_data.description or "UNKNOWN",
            line_data.hsn_number
        )
        
        return POLine(
            s_po_header_ref=po_header_ref,
            s_po_line_id=po_line_id,
            s_line_number=float(line_number),
            s_line_status="OPEN",
            s_item_ref=item_ref,
            s_hsn_id=line_data.hsn_number or "UNKNOWN",
            s_ordered_quantity=quantity,
            s_unit_price=unit_price,
            s_line_amount=line_amount,
            s_uom_id=line_data.unit or "EA",
            s_effective_from=effective_from,
            s_qc_required_flag=True,
            s_external_system="OCR_SYSTEM",
            s_external_system_id=f"OCR-LINE-{line_number}"
        )
    
    def _create_po_conditions(
        self,
        po_header_ref: UUID,
        igst: Optional[str],
        cgst: Optional[str],
        sgst: Optional[str],
        effective_from: date
    ) -> List[POCondition]:
        """Create PO Condition models for taxes"""
        conditions = []
        
        if igst:
            igst_rate = self.transformer.extract_tax_rate(igst)
            if igst_rate > 0:
                conditions.append(POCondition(
                    s_po_header_ref=po_header_ref,
                    s_po_condition_id=self.id_gen.generate_po_condition_id(
                        str(po_header_ref), "IGST"
                    ),
                    s_condition_type="IGST",
                    s_calculation_basis="PERCENTAGE",
                    s_rate=igst_rate,
                    s_uom_id="PERCENT",
                    s_effective_from=effective_from
                ))
        
        if cgst:
            cgst_rate = self.transformer.extract_tax_rate(cgst)
            if cgst_rate > 0:
                conditions.append(POCondition(
                    s_po_header_ref=po_header_ref,
                    s_po_condition_id=self.id_gen.generate_po_condition_id(
                        str(po_header_ref), "CGST"
                    ),
                    s_condition_type="CGST",
                    s_calculation_basis="PERCENTAGE",
                    s_rate=cgst_rate,
                    s_uom_id="PERCENT",
                    s_effective_from=effective_from
                ))
        
        if sgst:
            sgst_rate = self.transformer.extract_tax_rate(sgst)
            if sgst_rate > 0:
                conditions.append(POCondition(
                    s_po_header_ref=po_header_ref,
                    s_po_condition_id=self.id_gen.generate_po_condition_id(
                        str(po_header_ref), "SGST"
                    ),
                    s_condition_type="SGST",
                    s_calculation_basis="PERCENTAGE",
                    s_rate=sgst_rate,
                    s_uom_id="PERCENT",
                    s_effective_from=effective_from
                ))
        
        return conditions
    
    def _create_grn_header(
        self,
        grn_number: str,
        grn_id: str,
        grn_date: date,
        supplier_site_ref: UUID,
        legal_entity_site_ref: UUID,
        po_line_ref: UUID,
        total_qty: float,
        total_amount: float,
        effective_from: date
    ) -> GRNHeader:
        """Create GRN Header model"""
        return GRNHeader(
            s_grn_number=grn_number,
            s_grn_id=grn_id,
            s_grn_date=grn_date,
            s_grn_status="RECEIVED",
            s_qc_status="PENDING",
            s_supplier_site_ref=supplier_site_ref,
            s_legal_entity_site_ref=legal_entity_site_ref,
            s_po_line_ref=po_line_ref,
            s_gl_account_ref=self.resolver.resolve_gl_account_ref(),
            s_total_received_qty=total_qty,
            s_total_received_amount=total_amount,
            s_weight_uom_id="KG",
            s_effective_from=effective_from,
            s_external_system="OCR_SYSTEM",
            s_external_system_id=f"OCR-GRN-{grn_number}"
        )
    
    def _create_grn_line(
        self,
        grn_ref: UUID,
        line_number: int,
        line_data: InvoiceLine,
        grn_id: str,
        effective_from: date
    ) -> GRNLine:
        """Create GRN Line model"""
        grn_line_id = self.id_gen.generate_grn_line_id(grn_id, line_number)
        
        quantity = self.transformer.safe_float(line_data.quantity)
        unit_price = self.transformer.safe_float(line_data.unit_price)
        line_amount = self.transformer.safe_float(line_data.line_amount)
        
        # Resolve item reference
        item_ref = self.resolver.resolve_item_ref(
            line_data.description or "UNKNOWN",
            line_data.hsn_number
        )
        
        return GRNLine(
            s_grn_ref=grn_ref,
            s_grn_line_id=grn_line_id,
            s_item_description=line_data.description or "UNKNOWN",
            s_item_ref=item_ref,
            s_received_qty=quantity,
            s_unit_price=unit_price,
            s_total_received_amount=line_amount,
            s_accepted_qty=quantity,  # Initially accept all
            s_rejected_qty=0.0,
            s_uom_id=line_data.unit or "EA",
            s_weight_uom="KG",
            s_qc_result="PENDING",
            s_grn_line_status="RECEIVED",
            s_qc_required_flag=True,
            s_effective_from=effective_from,
            s_external_system="OCR_SYSTEM",
            s_external_system_id=f"OCR-GRNLINE-{line_number}"
        )
