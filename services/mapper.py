"""
Service for mapping OCR data to database models
Uses actual data from OCR input without generation
"""
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Dict, Any
from uuid import UUID
import random

from models.ocr_input import OCRInput, InvoiceLine, StaticData
from models.db_models import (
    GRNHeader, GRNLine, POHeader, POLine, POCondition
)
from utils.helpers import (
    IDGenerator, DataTransformer, ReferenceResolver
)
from logs.log import logger


class OCRMapper:
    """
    Maps OCR extracted data to database models
    Following SAP MM and Oracle Procurement Cloud standards
    """
    
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
        List[GRNLine],
        Dict[str, Any],
        Dict[str, Any],
        List[Dict[str, Any]]
    ]:
        """
        Map OCR data to all required database models
        
        Returns:
            Tuple of (po_header, po_lines, po_conditions, grn_header, grn_lines, 
                     supplier_info, buyer_info, item_info)
        """
        static = ocr_data.static
        lines = ocr_data.dynamic
        
        # Extract and validate common data - USE FROM INPUT, DON'T GENERATE
        invoice_no = self._extract_invoice_number(static)
        invoice_date = self._extract_invoice_date(static)
        po_number = self._extract_po_number(static, lines)  # FROM INPUT ONLY
        
        # Supplier information
        supplier_name = self.transformer.clean_string(
            self.transformer.extract_first(static.supplier_name),
            max_length=255
        ) or "Unknown Supplier"
        supplier_gstn = self.transformer.clean_string(
            self.transformer.extract_first(static.supplier_gstn),
            max_length=15
        )
        supplier_address = self.transformer.clean_string(
            self.transformer.extract_first(static.supplier_address),
            max_length=500
        )
        
        # Buyer information
        location_gstn = self.transformer.clean_string(
            self.transformer.extract_first(static.location_gstn),
            max_length=15
        )
        bill_to_address = self.transformer.clean_string(
            self.transformer.extract_first(static.bill_to_address),
            max_length=500
        )
        
        # Currency
        currency = self.resolver.resolve_currency_id(
            self.transformer.extract_first(static.invoice_currency) or "INR"
        )
        
        # Resolve master data references
        supplier_ref = self.resolver.resolve_supplier_ref(supplier_name, supplier_gstn)
        supplier_site_ref = self.resolver.resolve_site_ref(supplier_address, supplier_gstn)
        legal_entity_ref = self.resolver.resolve_legal_entity_ref(location_gstn)
        legal_entity_site_ref = self.resolver.resolve_site_ref(bill_to_address, location_gstn)
        
        # Prepare supplier info for master data insertion
        supplier_info = {
            'supplier_ref': supplier_ref,
            'supplier_name': supplier_name,
            'supplier_gstn': supplier_gstn,
            'supplier_address': supplier_address,
            'supplier_site_ref': supplier_site_ref
        }
        
        # Prepare buyer info for master data insertion
        buyer_info = {
            'legal_entity_ref': legal_entity_ref,
            'legal_entity_site_ref': legal_entity_site_ref,
            'location_gstn': location_gstn,
            'bill_to_address': bill_to_address
        }
        
        # Determine PO type based on item descriptions or default to MATERIAL
        po_type = self._determine_po_type(lines)
        
        # Prepare item info for master data insertion
        item_info = []
        
        # Create PO Header and Lines
        po_header = None
        po_lines = []
        po_line_refs = {}
        
        if po_number:
            # Use actual PO number from input - DON'T GENERATE
            po_id = self.id_gen.generate_po_id(po_number)
            
            # Calculate PO total
            po_total = sum(
                self.transformer.safe_float(line.line_amount)
                for line in lines
            )
            
            # Create PO Header
            po_header = self._create_po_header(
                po_number=po_number,  # FROM INPUT
                po_id=po_id,
                po_type=po_type,
                po_date=invoice_date,
                supplier_ref=supplier_ref,
                supplier_site_ref=supplier_site_ref,
                legal_entity_ref=legal_entity_ref,
                legal_entity_site_ref=legal_entity_site_ref,
                currency=currency,
                total_value=po_total
            )
            
            # Create PO Lines and collect item info
            for idx, line in enumerate(lines, start=1):
                hsn_code = self.transformer.extract_hsn_code(line.hsn_number)
                item_description = line.description or "UNKNOWN"
                uom = self.transformer.normalize_uom(line.unit)
                
                item_ref = self.resolver.resolve_item_ref(item_description, hsn_code)
                
                # Add to item info for master data insertion
                item_info.append({
                    'item_ref': item_ref,
                    'description': item_description,
                    'hsn_code': hsn_code,
                    'uom': uom
                })
                
                po_line = self._create_po_line(
                    po_header_ref=po_header.id,
                    po_id=po_id,
                    line_number=idx,
                    line_data=line,
                    effective_from=invoice_date,
                    po_date=invoice_date,
                    item_ref=item_ref,
                    hsn_code=hsn_code,
                    uom=uom
                )
                po_lines.append(po_line)
                po_line_refs[idx] = po_line.id
        else:
            # No PO number - still need to collect item info for GRN
            for idx, line in enumerate(lines, start=1):
                hsn_code = self.transformer.extract_hsn_code(line.hsn_number)
                item_description = line.description or "UNKNOWN"
                uom = self.transformer.normalize_uom(line.unit)
                
                item_ref = self.resolver.resolve_item_ref(item_description, hsn_code)
                
                item_info.append({
                    'item_ref': item_ref,
                    'description': item_description,
                    'hsn_code': hsn_code,
                    'uom': uom
                })
        
        # Create PO Conditions (Tax)
        po_conditions = []
        if po_header:
            po_conditions = self._create_po_conditions(
                po_header_ref=po_header.id,
                po_id=po_header.s_po_id,
                static=static,
                lines=lines,
                effective_from=invoice_date
            )
        
        # Generate GRN identifiers
        grn_number = self.id_gen.generate_grn_number()
        grn_id = self.id_gen.generate_grn_id(grn_number)
        
        # Calculate GRN totals
        total_qty = sum(
            self.transformer.safe_float(line.quantity) 
            for line in lines
        )
        total_amount = self.transformer.safe_float(
            self.transformer.extract_first(static.subtotal) or
            self.transformer.extract_first(static.total_invoice_amount)
        )
        
        # Create GRN Header
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
                grn_id=grn_id,
                line_number=idx,
                line_data=line,
                effective_from=invoice_date,
                item_info=item_info[idx-1]
            )
            grn_lines.append(grn_line)
        
        logger.info(
            "Mapped OCR data: invoice=%s, po=%s, grn=%s, lines=%d",
            invoice_no, po_number or "N/A", grn_number, len(lines)
        )
        
        return (po_header, po_lines, po_conditions, grn_header, grn_lines,
                supplier_info, buyer_info, item_info)
    
    def _extract_invoice_number(self, static: StaticData) -> str:
        """Extract and clean invoice number"""
        invoice_no = self.transformer.extract_first(static.invoice_no)
        if not invoice_no:
            # Generate fallback invoice number
            invoice_no = f"INV{datetime.now().strftime('%Y%m%d%H%M%S')}"
        return self.transformer.clean_string(invoice_no, max_length=50) or "UNKNOWN"
    
    def _extract_invoice_date(self, static: StaticData) -> date:
        """Extract and parse invoice date"""
        date_str = self.transformer.extract_first(static.invoice_date)
        parsed_date = self.transformer.parse_date(date_str)
        return parsed_date or date.today()
    
    def _extract_po_number(self, static: StaticData, lines: List[InvoiceLine]) -> Optional[str]:
        """Extract PO number from static or line data - NEVER GENERATE"""
        # Try static first
        po_number = self.transformer.extract_first(static.po_number)
        
        # If not in static, try first line
        if not po_number and lines:
            po_number = lines[0].po_number
        
        if po_number:
            return self.transformer.clean_string(po_number, max_length=50)
        
        # Return None if not found - DON'T GENERATE
        return None
    
    def _determine_po_type(self, lines: List[InvoiceLine]) -> str:
        """
        Determine PO type based on line items
        Returns: MATERIAL, SERVICE, or CAPEX
        """
        if not lines:
            return "MATERIAL"
        
        # Check descriptions for keywords
        descriptions = ' '.join(
            (line.description or '').lower() 
            for line in lines
        )
        
        # Service indicators
        service_keywords = ['service', 'consulting', 'maintenance', 'support', 'license']
        if any(keyword in descriptions for keyword in service_keywords):
            return "SERVICE"
        
        # CAPEX indicators  
        capex_keywords = ['equipment', 'machinery', 'capital', 'installation', 'infrastructure']
        if any(keyword in descriptions for keyword in capex_keywords):
            return "CAPEX"
        
        # Default to MATERIAL
        return "MATERIAL"
    
    def _create_po_header(
        self,
        po_number: str,
        po_id: str,
        po_type: str,
        po_date: date,
        supplier_ref: UUID,
        supplier_site_ref: UUID,
        legal_entity_ref: UUID,
        legal_entity_site_ref: UUID,
        currency: str,
        total_value: float
    ) -> POHeader:
        """Create PO Header model with realistic data"""
        
        # Payment terms based on PO type
        payment_terms_map = {
            'MATERIAL': '0002',  # SAP code for Net 30
            'SERVICE': '0003',   # Net 45
            'CAPEX': '0004'      # Net 60
        }
        payment_terms = payment_terms_map.get(po_type, '0002')
        
        # Matching type
        matching_type = '3WAY' if po_type == 'MATERIAL' else '2WAY'
        
        # Incoterms based on typical scenarios
        incoterms = 'EXW' if po_type == 'CAPEX' else 'DDP'
        
        return POHeader(
            s_po_number=po_number,  # FROM INPUT
            s_po_id=po_id,
            s_po_date=po_date,
            s_po_status='APPROVED',
            s_po_type=po_type,
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
            s_created_by='OCR_AUTOMATION',
            s_payment_terms=payment_terms,
            s_matching_type=matching_type,
            s_effective_from=po_date,
            s_po_valid_from=po_date,
            s_po_valid_to=po_date + timedelta(days=365),
            s_incoterms=incoterms,
            s_freight_included_flag=(po_type == 'MATERIAL'),
            s_external_system='INVOICE_OCR',
            s_external_system_id=po_number[:10]
        )
    
    def _create_po_line(
        self,
        po_header_ref: UUID,
        po_id: str,
        line_number: int,
        line_data: InvoiceLine,
        effective_from: date,
        po_date: date,
        item_ref: UUID,
        hsn_code: str,
        uom: str
    ) -> POLine:
        """Create PO Line model with realistic data"""
        
        po_line_id = self.id_gen.generate_po_line_id(po_id, line_number)
        
        # Extract and validate amounts
        quantity = self.transformer.safe_float(line_data.quantity, precision=3)
        unit_price = self.transformer.safe_float(line_data.unit_price, precision=4)
        line_amount = self.transformer.safe_float(line_data.line_amount, precision=2)
        
        # Validate line amount
        if line_amount == 0 and quantity > 0 and unit_price > 0:
            line_amount = round(quantity * unit_price, 2)
        
        # Expected delivery date (typical lead time: 14 days)
        expected_delivery = self.transformer.calculate_expected_delivery_date(po_date, 14)
        
        # Tolerance percentage (typical: 5-10%)
        tolerance_pct = 5.0 if quantity < 100 else 10.0
        
        return POLine(
            s_po_header_ref=po_header_ref,
            s_po_line_id=po_line_id,
            s_line_number=float(line_number * 10),
            s_line_status='OPEN',
            s_item_ref=item_ref,
            s_hsn_id=hsn_code,
            s_ordered_quantity=quantity,
            s_unit_price=unit_price,
            s_line_amount=line_amount,
            s_uom_id=uom,
            s_effective_from=effective_from,
            s_expected_delivery_date=expected_delivery,
            s_qc_required_flag=True,
            s_batch_required_flag=False,
            s_expiry_required_flag=False,
            s_coa_required_flag=False,
            s_tolerance_pct=tolerance_pct,
            s_external_system='INVOICE_OCR',
            s_external_system_id=str(line_number)[:10]
        )
    
    def _create_po_conditions(
        self,
        po_header_ref: UUID,
        po_id: str,
        static: StaticData,
        lines: List[InvoiceLine],
        effective_from: date
    ) -> List[POCondition]:
        """Create PO Condition models for taxes with realistic data"""
        conditions = []
        
        # Helper to create condition
        def add_condition(tax_type: str, rate_str: Optional[str]):
            if not rate_str:
                return
            
            rate = self.transformer.extract_tax_rate(rate_str)
            if rate > 0:
                condition_id = self.id_gen.generate_po_condition_id(po_id, tax_type)
                
                conditions.append(POCondition(
                    s_po_header_ref=po_header_ref,
                    s_po_condition_id=condition_id,
                    s_condition_type=tax_type,
                    s_calculation_basis='PERCENT',
                    s_rate=rate,
                    s_uom_id='%',
                    s_effective_from=effective_from,
                    s_external_system='INVOICE_OCR',
                    s_external_system_id=tax_type[:10]
                ))
        
        # Process tax conditions from static data
        add_condition('IGST', self.transformer.extract_first(static.igst))
        add_condition('CGST', self.transformer.extract_first(static.cgst))
        add_condition('SGST', self.transformer.extract_first(static.sgst))
        
        # Also check line-level tax rates
        for line in lines:
            if line.igst_rate:
                add_condition('IGST', line.igst_rate)
            if line.cgst_rate:
                add_condition('CGST', line.cgst_rate)
            if line.sgst_rate:
                add_condition('SGST', line.sgst_rate)
        
        # Remove duplicates (keep first occurrence)
        seen = set()
        unique_conditions = []
        for cond in conditions:
            key = (cond.s_condition_type, cond.s_rate)
            if key not in seen:
                seen.add(key)
                unique_conditions.append(cond)
        
        return unique_conditions
    
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
        """Create GRN Header model with realistic data"""
        
        grn_status = 'POSTED'
        qc_status = 'PENDING'
        transport_mode = 'ROAD'
        
        return GRNHeader(
            s_grn_number=grn_number,
            s_grn_id=grn_id,
            s_grn_date=grn_date,
            s_grn_status=grn_status,
            s_qc_status=qc_status,
            s_supplier_site_ref=supplier_site_ref,
            s_legal_entity_site_ref=legal_entity_site_ref,
            s_po_line_ref=po_line_ref,
            s_gl_account_ref=self.resolver.resolve_gl_account_ref(),
            s_total_received_qty=total_qty,
            s_total_received_amount=total_amount,
            s_weight_uom_id='KG',
            s_transport_mode=transport_mode,
            s_effective_from=effective_from,
            s_external_system='INVOICE_OCR',
            s_external_system_id=grn_number[:10]
        )
    
    def _create_grn_line(
        self,
        grn_ref: UUID,
        grn_id: str,
        line_number: int,
        line_data: InvoiceLine,
        effective_from: date,
        item_info: Dict[str, Any]
    ) -> GRNLine:
        """Create GRN Line model with realistic data"""
        
        grn_line_id = self.id_gen.generate_grn_line_id(grn_id, line_number)
        
        # Extract quantities
        received_qty = self.transformer.safe_float(line_data.quantity, precision=3)
        unit_price = self.transformer.safe_float(line_data.unit_price, precision=4)
        line_amount = self.transformer.safe_float(line_data.line_amount, precision=2)
        
        # Validate line amount
        if line_amount == 0 and received_qty > 0 and unit_price > 0:
            line_amount = round(received_qty * unit_price, 2)
        
        # Initial acceptance (100% - no rejection at receipt)
        accepted_qty = received_qty
        rejected_qty = 0.0
        
        # Generate batch number for materials
        batch_number = self.id_gen.generate_batch_number(
            material_code=item_info['hsn_code'],
            manufacture_date=effective_from
        )
        
        # QC result - initially pending
        qc_result = 'PENDING'
        grn_line_status = 'RECEIVED'
        
        return GRNLine(
            s_grn_ref=grn_ref,
            s_grn_line_id=grn_line_id,
            s_item_description=self.transformer.clean_string(
                line_data.description or "UNKNOWN",
                max_length=255
            ) or "UNKNOWN",
            s_item_ref=item_info['item_ref'],
            s_received_qty=received_qty,
            s_unit_price=unit_price,
            s_total_received_amount=line_amount,
            s_accepted_qty=accepted_qty,
            s_rejected_qty=rejected_qty,
            s_uom_id=item_info['uom'],
            s_weight_uom='KG',
            s_qc_result=qc_result,
            s_grn_line_status=grn_line_status,
            s_qc_required_flag=True,
            s_batch_number=batch_number,
            s_manufacture_date=effective_from,
            s_expiry_date=None,
            s_compliance_verified_flag=False,
            s_effective_from=effective_from,
            s_external_system='INVOICE_OCR',
            s_external_system_id=str(line_number)[:10]
        )
