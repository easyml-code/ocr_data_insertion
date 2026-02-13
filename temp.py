"""
Database service for inserting GRN and PO data
FIXED: Tax rates handled by name instead of UUID caching
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime, date

from models.db_models import (
    GRNHeader, GRNLine, POHeader, POLine, POCondition
)
from services.master_data import MasterDataService
from logs.log import logger


class DatabaseService:
    """Service for database operations"""
    
    def __init__(self, run_query_func):
        """
        Initialize with the run_query function
        
        Args:
            run_query_func: Async function to execute database queries
        """
        self.run_query = run_query_func
        self.schema = "tenant_data"
        self.master_data_service = MasterDataService(run_query_func)
    
    async def insert_complete_invoice(
        self,
        po_header: Optional[POHeader],
        po_lines: List[POLine],
        po_conditions: List[POCondition],
        grn_header: GRNHeader,
        grn_lines: List[GRNLine],
        supplier_info: Dict[str, Any],
        buyer_info: Dict[str, Any],
        item_info: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Insert complete invoice data (PO and GRN) into database
        First ensures all master data exists, then inserts transactional data
        Returns summary of inserted records
        """
        results = {
            "po_header_id": None,
            "po_lines_count": 0,
            "po_conditions_count": 0,
            "grn_header_id": str(grn_header.id),
            "grn_lines_count": 0,
            "success": False,
            "errors": []
        }
        
        try:
            # Step 1: Ensure all master data exists
            logger.info("=== MASTER DATA INSERTION PHASE ===")
            logger.info("Ensuring master data exists...")
            
            # Ensure supplier and supplier site
            logger.info(f"Ensuring supplier exists: name='{supplier_info.get('supplier_name')}', ref={supplier_info['supplier_ref']}")
            await self.master_data_service.ensure_supplier(
                supplier_ref=supplier_info['supplier_ref'],
                supplier_name=supplier_info['supplier_name'],
                supplier_gstn=supplier_info.get('supplier_gstn')
            )
            logger.info(f"✓ Supplier ensured successfully")
            
            logger.info(f"Ensuring supplier site exists: ref={supplier_info['supplier_site_ref']}, gstin={supplier_info.get('supplier_gstn')}")
            await self.master_data_service.ensure_supplier_site(
                site_ref=supplier_info['supplier_site_ref'],
                supplier_ref=supplier_info['supplier_ref'],
                address=supplier_info.get('supplier_address'),
                gstin=supplier_info.get('supplier_gstn')
            )
            logger.info(f"✓ Supplier site ensured successfully")
            
            # Ensure legal entity and legal entity site
            logger.info(f"Ensuring legal entity exists: ref={buyer_info['legal_entity_ref']}, gstin={buyer_info.get('location_gstn')}")
            await self.master_data_service.ensure_legal_entity(
                entity_ref=buyer_info['legal_entity_ref'],
                gstin=buyer_info.get('location_gstn')
            )
            logger.info(f"✓ Legal entity ensured successfully")
            
            logger.info(f"Ensuring legal entity site exists: ref={buyer_info['legal_entity_site_ref']}")
            await self.master_data_service.ensure_legal_entity_site(
                site_ref=buyer_info['legal_entity_site_ref'],
                entity_ref=buyer_info['legal_entity_ref'],
                address=buyer_info.get('bill_to_address'),
                gstin=buyer_info.get('location_gstn')
            )
            logger.info(f"✓ Legal entity site ensured successfully")
            
            # Ensure all items exist
            logger.info(f"Ensuring {len(item_info)} items exist...")
            for idx, item in enumerate(item_info, 1):
                logger.debug(f"Ensuring item {idx}/{len(item_info)}: {item['description'][:50]}...")
                await self.master_data_service.ensure_item(
                    item_ref=item['item_ref'],
                    description=item['description'],
                    hsn_code=item['hsn_code'],
                    uom=item.get('uom', 'EA')
                )
            logger.info(f"✓ All {len(item_info)} items ensured successfully")
            
            # If PO exists, ensure its master data
            if po_header:
                logger.info(f"Ensuring PO master data for PO: {po_header.s_po_number}")
                logger.debug(f"Ensuring cost center: {po_header.s_cost_center_ref}")
                await self.master_data_service.ensure_cost_center(po_header.s_cost_center_ref)
                logger.debug(f"Ensuring profit center: {po_header.s_profit_center_ref}")
                await self.master_data_service.ensure_profit_center(po_header.s_profit_center_ref)
                logger.debug(f"Ensuring project: {po_header.s_project_ref}")
                await self.master_data_service.ensure_project(po_header.s_project_ref)
                logger.debug(f"Ensuring plant: {po_header.s_plant_ref}")
                await self.master_data_service.ensure_plant(po_header.s_plant_ref)
                
                # Ensure tax rate by name (not UUID)
                logger.debug(f"Ensuring tax rate: {po_header.s_tax_rate_ref}")
                tax_rate_uuid = await self.master_data_service.ensure_tax_rate(po_header.s_tax_rate_ref)
                # Update PO header to use the UUID returned from master data service
                po_header.s_tax_rate_ref = tax_rate_uuid
                logger.info(f"✓ PO master data ensured successfully")
            
            # Ensure GL account for GRN
            logger.info(f"Ensuring GL account: {grn_header.s_gl_account_ref}")
            await self.master_data_service.ensure_gl_account(grn_header.s_gl_account_ref)
            logger.info(f"✓ GL account ensured successfully")
            
            logger.info("=== ALL MASTER DATA ENSURED SUCCESSFULLY ===")
            
            # Step 2: Check if PO exists, insert only if new
            logger.info("=== TRANSACTIONAL DATA INSERTION PHASE ===")
            if po_header:
                # CRITICAL: Check if PO already exists (multiple invoices can reference same PO)
                existing_po = await self._check_po_exists(po_header.s_po_number)
                
                if existing_po:
                    logger.info(f"PO already exists: {po_header.s_po_number}, using existing PO data")
                    results["po_header_id"] = existing_po["po_header_id"]
                    results["po_exists"] = True
                    
                    # Get first PO line from existing PO for GRN reference
                    first_po_line = await self._get_first_po_line(existing_po["po_header_id"])
                    if first_po_line:
                        logger.info(f"Using existing PO line for GRN: {first_po_line['id']}")
                        grn_header.s_po_line_ref = UUID(first_po_line['id'])
                    else:
                        logger.warning(f"No PO lines found for existing PO {po_header.s_po_number}")
                    
                    logger.info(f"✓ Using existing PO: {po_header.s_po_number}")
                else:
                    # PO doesn't exist, insert new PO with all its data
                    logger.info(f"Inserting new PO Header: {po_header.s_po_number}")
                    await self._insert_po_header(po_header)
                    results["po_header_id"] = str(po_header.id)
                    logger.info(f"✓ PO Header inserted: {po_header.s_po_number}")
                    
                    # Insert PO Lines
                    logger.info(f"Inserting {len(po_lines)} PO Lines...")
                    for idx, po_line in enumerate(po_lines, 1):
                        logger.debug(f"Inserting PO Line {idx}/{len(po_lines)}: {po_line.s_po_line_id}")
                        await self._insert_po_line(po_line)
                        results["po_lines_count"] += 1
                    logger.info(f"✓ {len(po_lines)} PO Lines inserted")
                    
                    # Insert PO Conditions
                    if po_conditions:
                        logger.info(f"Inserting {len(po_conditions)} PO Conditions...")
                        for idx, po_condition in enumerate(po_conditions, 1):
                            logger.debug(f"Inserting PO Condition {idx}/{len(po_conditions)}: {po_condition.s_condition_type}")
                            await self._insert_po_condition(po_condition)
                            results["po_conditions_count"] += 1
                        logger.info(f"✓ {len(po_conditions)} PO Conditions inserted")
                    
                    results["po_exists"] = False
            else:
                logger.info("No PO data to insert (invoice without PO reference)")
            
            # Step 3: Insert GRN Header
            logger.info(f"Inserting GRN Header: {grn_header.s_grn_number}")
            await self._insert_grn_header(grn_header)
            logger.info(f"✓ GRN Header inserted: {grn_header.s_grn_number}")
            
            # Step 4: Insert GRN Lines
            logger.info(f"Inserting {len(grn_lines)} GRN Lines...")
            for idx, grn_line in enumerate(grn_lines, 1):
                logger.debug(f"Inserting GRN Line {idx}/{len(grn_lines)}: {grn_line.s_grn_line_id}")
                await self._insert_grn_line(grn_line)
                results["grn_lines_count"] += 1
            logger.info(f"✓ {len(grn_lines)} GRN Lines inserted")
            
            results["success"] = True
            logger.info("=== INVOICE INSERTION COMPLETED SUCCESSFULLY ===")
            
        except Exception as e:
            logger.exception(f"❌ ERROR during invoice data insertion: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            results["errors"].append(str(e))
            raise
        
        return results
    
    async def _check_po_exists(self, po_number: str) -> Optional[Dict[str, Any]]:
        """
        Check if PO already exists in database by po_number
        
        Args:
            po_number: PO number to check
            
        Returns:
            Dictionary with po_header_id if exists, None otherwise
        """
        try:
            query = f"""
                SELECT id, s_po_id 
                FROM {self.schema}.s_po_header 
                WHERE s_po_number = '{self._escape_string(po_number)}'
                LIMIT 1;
            """
            result = await self.run_query(query)
            
            if result and len(result) > 0:
                logger.info(f"Found existing PO: {po_number}, ID: {result[0]['id']}")
                return {
                    "po_header_id": result[0]['id'],
                    "po_id": result[0]['s_po_id']
                }
            
            logger.info(f"PO not found in database: {po_number}, will create new")
            return None
            
        except Exception as e:
            logger.error(f"Error checking if PO exists: {e}")
            # If error checking, assume doesn't exist to attempt insert
            return None
    
    async def _get_first_po_line(self, po_header_id: str) -> Optional[Dict[str, Any]]:
        """
        Get first PO line for a given PO header
        
        Args:
            po_header_id: PO header UUID
            
        Returns:
            Dictionary with po_line id if found, None otherwise
        """
        try:
            query = f"""
                SELECT id, s_po_line_id 
                FROM {self.schema}.s_po_line 
                WHERE s_po_header_ref = '{po_header_id}'
                ORDER BY s_line_number ASC
                LIMIT 1;
            """
            result = await self.run_query(query)
            
            if result and len(result) > 0:
                logger.debug(f"Found first PO line: {result[0]['id']}")
                return {
                    "id": result[0]['id'],
                    "po_line_id": result[0]['s_po_line_id']
                }
            
            logger.warning(f"No PO lines found for PO header: {po_header_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting first PO line: {e}")
            return None
    
    async def _insert_po_header(self, po_header: POHeader) -> None:
        """Insert PO Header into database"""
        try:
            query = f"""
                INSERT INTO {self.schema}.s_po_header (
                    id, is_deleted, created_at, updated_at, created_by, updated_by,
                    s_approved_by, s_cost_center_ref, s_created_by, s_currency_id,
                    s_effective_from, s_effective_to, s_external_system, s_external_system_id,
                    s_freight_included_flag, s_incoterms, s_legal_entity_ref, s_legal_entity_site_ref,
                    s_matching_type, s_payment_terms, s_plant_ref, s_po_date,
                    s_po_id, s_po_number, s_po_status, s_po_total_value,
                    s_po_type, s_po_valid_from, s_po_valid_to, s_profit_center_ref,
                    s_project_id, s_project_ref, s_supplier_ref, s_supplier_site_ref,
                    s_tax_rate_ref
                ) VALUES (
                    '{po_header.id}', {po_header.is_deleted}, 
                    '{self._format_timestamp(po_header.created_at)}',
                    '{self._format_timestamp(po_header.updated_at)}',
                    {self._format_uuid(po_header.created_by)},
                    {self._format_uuid(po_header.updated_by)},
                    {self._format_string(po_header.s_approved_by)},
                    '{po_header.s_cost_center_ref}',
                    '{po_header.s_created_by}',
                    '{po_header.s_currency_id}',
                    '{po_header.s_effective_from}',
                    {self._format_date(po_header.s_effective_to)},
                    {self._format_string(po_header.s_external_system)},
                    {self._format_string(po_header.s_external_system_id)},
                    {po_header.s_freight_included_flag},
                    {self._format_string(po_header.s_incoterms)},
                    '{po_header.s_legal_entity_ref}',
                    '{po_header.s_legal_entity_site_ref}',
                    '{po_header.s_matching_type}',
                    '{po_header.s_payment_terms}',
                    '{po_header.s_plant_ref}',
                    '{po_header.s_po_date}',
                    '{po_header.s_po_id}',
                    '{po_header.s_po_number}',
                    '{po_header.s_po_status}',
                    {po_header.s_po_total_value},
                    '{po_header.s_po_type}',
                    {self._format_date(po_header.s_po_valid_from)},
                    {self._format_date(po_header.s_po_valid_to)},
                    '{po_header.s_profit_center_ref}',
                    {self._format_string(po_header.s_project_id)},
                    '{po_header.s_project_ref}',
                    '{po_header.s_supplier_ref}',
                    '{po_header.s_supplier_site_ref}',
                    '{po_header.s_tax_rate_ref}'
                );
            """
            await self.run_query(query)
        except Exception as e:
            logger.error(f"Failed to insert PO header {po_header.s_po_number}: {e}")
            raise
    
    async def _insert_po_line(self, po_line: POLine) -> None:
        """Insert PO Line into database"""
        try:
            query = f"""
                INSERT INTO {self.schema}.s_po_line (
                    id, is_deleted, created_at, updated_at, created_by, updated_by,
                    s_batch_required_flag, s_cas_number, s_chemical_grade, s_closed_quantity,
                    s_coa_required_flag, s_drawing_number, s_drawing_revision, s_effective_from,
                    s_effective_to, s_expected_delivery_date, s_expiry_required_flag,
                    s_external_system, s_external_system_id, s_hsn_id, s_item_ref,
                    s_line_amount, s_line_number, s_line_status, s_ordered_quantity,
                    s_po_header_ref, s_po_line_id, s_price_valid_from, s_price_valid_to,
                    s_qc_required_flag, s_tolerance_pct, s_total_invoiced_qty,
                    s_unit_price, s_uom_id
                ) VALUES (
                    '{po_line.id}', {po_line.is_deleted},
                    '{self._format_timestamp(po_line.created_at)}',
                    '{self._format_timestamp(po_line.updated_at)}',
                    {self._format_uuid(po_line.created_by)},
                    {self._format_uuid(po_line.updated_by)},
                    {po_line.s_batch_required_flag},
                    {self._format_string(po_line.s_cas_number)},
                    {self._format_string(po_line.s_chemical_grade)},
                    {self._format_numeric(po_line.s_closed_quantity)},
                    {po_line.s_coa_required_flag},
                    {self._format_string(po_line.s_drawing_number)},
                    {self._format_string(po_line.s_drawing_revision)},
                    '{po_line.s_effective_from}',
                    {self._format_date(po_line.s_effective_to)},
                    {self._format_date(po_line.s_expected_delivery_date)},
                    {po_line.s_expiry_required_flag},
                    {self._format_string(po_line.s_external_system)},
                    {self._format_string(po_line.s_external_system_id)},
                    '{po_line.s_hsn_id}',
                    '{po_line.s_item_ref}',
                    {self._format_numeric(po_line.s_line_amount)},
                    {po_line.s_line_number},
                    '{po_line.s_line_status}',
                    {po_line.s_ordered_quantity},
                    '{po_line.s_po_header_ref}',
                    '{po_line.s_po_line_id}',
                    {self._format_date(po_line.s_price_valid_from)},
                    {self._format_date(po_line.s_price_valid_to)},
                    {po_line.s_qc_required_flag},
                    {self._format_numeric(po_line.s_tolerance_pct)},
                    {self._format_numeric(po_line.s_total_invoiced_qty)},
                    {po_line.s_unit_price},
                    '{po_line.s_uom_id}'
                );
            """
            await self.run_query(query)
        except Exception as e:
            logger.error(f"Failed to insert PO line {po_line.s_po_line_id}: {e}")
            raise
    
    async def _insert_po_condition(self, po_condition: POCondition) -> None:
        """Insert PO Condition into database"""
        try:
            query = f"""
                INSERT INTO {self.schema}.s_po_condition (
                    id, is_deleted, created_at, updated_at, created_by, updated_by,
                    s_calculation_basis, s_condition_type, s_effective_from, s_effective_to,
                    s_external_system, s_external_system_id, s_po_condition_id,
                    s_po_header_ref, s_rate, s_uom_id
                ) VALUES (
                    '{po_condition.id}', {po_condition.is_deleted},
                    '{self._format_timestamp(po_condition.created_at)}',
                    '{self._format_timestamp(po_condition.updated_at)}',
                    {self._format_uuid(po_condition.created_by)},
                    {self._format_uuid(po_condition.updated_by)},
                    '{po_condition.s_calculation_basis}',
                    '{po_condition.s_condition_type}',
                    '{po_condition.s_effective_from}',
                    {self._format_date(po_condition.s_effective_to)},
                    {self._format_string(po_condition.s_external_system)},
                    {self._format_string(po_condition.s_external_system_id)},
                    '{po_condition.s_po_condition_id}',
                    '{po_condition.s_po_header_ref}',
                    {po_condition.s_rate},
                    '{po_condition.s_uom_id}'
                );
            """
            await self.run_query(query)
        except Exception as e:
            logger.error(f"Failed to insert PO condition {po_condition.s_po_condition_id}: {e}")
            raise
    
    async def _insert_grn_header(self, grn_header: GRNHeader) -> None:
        """Insert GRN Header into database"""
        try:
            query = f"""
                INSERT INTO {self.schema}.s_grn_header (
                    id, is_deleted, created_at, updated_at, created_by, updated_by,
                    s_effective_from, s_effective_to, s_external_system, s_external_system_id,
                    s_gl_account_ref, s_grn_date, s_grn_id, s_grn_number,
                    s_grn_status, s_legal_entity_site_ref, s_po_line_ref, s_qc_status,
                    s_supplier_site_ref, s_total_received_amount, s_total_received_qty,
                    s_total_received_weight, s_transport_mode, s_weight_uom_id
                ) VALUES (
                    '{grn_header.id}', {grn_header.is_deleted},
                    '{self._format_timestamp(grn_header.created_at)}',
                    '{self._format_timestamp(grn_header.updated_at)}',
                    {self._format_uuid(grn_header.created_by)},
                    {self._format_uuid(grn_header.updated_by)},
                    '{grn_header.s_effective_from}',
                    {self._format_date(grn_header.s_effective_to)},
                    {self._format_string(grn_header.s_external_system)},
                    {self._format_string(grn_header.s_external_system_id)},
                    '{grn_header.s_gl_account_ref}',
                    '{grn_header.s_grn_date}',
                    '{grn_header.s_grn_id}',
                    '{grn_header.s_grn_number}',
                    '{grn_header.s_grn_status}',
                    '{grn_header.s_legal_entity_site_ref}',
                    '{grn_header.s_po_line_ref}',
                    '{grn_header.s_qc_status}',
                    '{grn_header.s_supplier_site_ref}',
                    {grn_header.s_total_received_amount},
                    {grn_header.s_total_received_qty},
                    {self._format_numeric(grn_header.s_total_received_weight)},
                    {self._format_string(grn_header.s_transport_mode)},
                    '{grn_header.s_weight_uom_id}'
                );
            """
            await self.run_query(query)
        except Exception as e:
            logger.error(f"Failed to insert GRN header {grn_header.s_grn_number}: {e}")
            raise
    
    async def _insert_grn_line(self, grn_line: GRNLine) -> None:
        """Insert GRN Line into database"""
        try:
            query = f"""
                INSERT INTO {self.schema}.s_grn_line (
                    id, is_deleted, created_at, updated_at, created_by, updated_by,
                    s_grn_line_id, s_item_description, s_drawing_number, s_drawing_revision,
                    s_uom_id, s_received_qty, s_unit_price, s_total_received_amount,
                    s_accepted_qty, s_rejected_qty, s_rejection_reason, s_received_weight,
                    s_weight_uom, s_qc_required_flag, s_qc_result, s_grn_line_status,
                    s_batch_number, s_manufacture_date, s_expiry_date, s_compliance_verified_flag,
                    s_grn_ref, s_item_ref, s_effective_from, s_effective_to,
                    s_external_system_id, s_external_system
                ) VALUES (
                    '{grn_line.id}', {grn_line.is_deleted},
                    '{self._format_timestamp(grn_line.created_at)}',
                    '{self._format_timestamp(grn_line.updated_at)}',
                    {self._format_uuid(grn_line.created_by)},
                    {self._format_uuid(grn_line.updated_by)},
                    '{grn_line.s_grn_line_id}',
                    '{self._escape_string(grn_line.s_item_description)}',
                    {self._format_string(grn_line.s_drawing_number)},
                    {self._format_string(grn_line.s_drawing_revision)},
                    '{grn_line.s_uom_id}',
                    {grn_line.s_received_qty},
                    {self._format_numeric(grn_line.s_unit_price)},
                    {self._format_numeric(grn_line.s_total_received_amount)},
                    {self._format_numeric(grn_line.s_accepted_qty)},
                    {self._format_numeric(grn_line.s_rejected_qty)},
                    {self._format_string(grn_line.s_rejection_reason)},
                    {self._format_numeric(grn_line.s_received_weight)},
                    '{grn_line.s_weight_uom}',
                    {grn_line.s_qc_required_flag},
                    '{grn_line.s_qc_result}',
                    '{grn_line.s_grn_line_status}',
                    {self._format_string(grn_line.s_batch_number)},
                    {self._format_date(grn_line.s_manufacture_date)},
                    {self._format_date(grn_line.s_expiry_date)},
                    {grn_line.s_compliance_verified_flag},
                    '{grn_line.s_grn_ref}',
                    '{grn_line.s_item_ref}',
                    '{grn_line.s_effective_from}',
                    {self._format_date(grn_line.s_effective_to)},
                    {self._format_string(grn_line.s_external_system_id)},
                    {self._format_string(grn_line.s_external_system)}
                );
            """
            await self.run_query(query)
        except Exception as e:
            logger.error(f"Failed to insert GRN line {grn_line.s_grn_line_id}: {e}")
            raise
    
    # Helper formatting methods
    
    def _format_timestamp(self, dt: datetime) -> str:
        """Format datetime for PostgreSQL"""
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    def _format_date(self, d: Optional[date]) -> str:
        """Format date for PostgreSQL"""
        if d is None:
            return "NULL"
        return f"'{d}'"
    
    def _format_uuid(self, uuid_val: Optional[UUID]) -> str:
        """Format UUID for PostgreSQL"""
        if uuid_val is None:
            return "NULL"
        return f"'{uuid_val}'"
    
    def _format_string(self, s: Optional[str]) -> str:
        """Format string for PostgreSQL"""
        if s is None:
            return "NULL"
        return f"'{self._escape_string(s)}'"
    
    def _format_numeric(self, n: Optional[float]) -> str:
        """Format numeric value for PostgreSQL"""
        if n is None:
            return "NULL"
        return str(n)
    
    def _escape_string(self, s: str) -> str:
        """Escape single quotes in strings"""
        return s.replace("'", "''")
