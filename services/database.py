"""
Database service for inserting GRN and PO data
"""
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime, date

from models.db_models import (
    GRNHeader, GRNLine, POHeader, POLine, POCondition
)
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
    
    async def insert_complete_invoice(
        self,
        po_header: Optional[POHeader],
        po_lines: List[POLine],
        po_conditions: List[POCondition],
        grn_header: GRNHeader,
        grn_lines: List[GRNLine]
    ) -> Dict[str, Any]:
        """
        Insert complete invoice data (PO and GRN) into database
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
            # Insert PO Header if exists
            if po_header:
                await self._insert_po_header(po_header)
                results["po_header_id"] = str(po_header.id)
                logger.info(f"Inserted PO Header: {po_header.s_po_number}")
                
                # Insert PO Lines
                for po_line in po_lines:
                    await self._insert_po_line(po_line)
                    results["po_lines_count"] += 1
                logger.info(f"Inserted {len(po_lines)} PO Lines")
                
                # Insert PO Conditions
                for po_condition in po_conditions:
                    await self._insert_po_condition(po_condition)
                    results["po_conditions_count"] += 1
                logger.info(f"Inserted {len(po_conditions)} PO Conditions")
            
            # Insert GRN Header
            await self._insert_grn_header(grn_header)
            logger.info(f"Inserted GRN Header: {grn_header.s_grn_number}")
            
            # Insert GRN Lines
            for grn_line in grn_lines:
                await self._insert_grn_line(grn_line)
                results["grn_lines_count"] += 1
            logger.info(f"Inserted {len(grn_lines)} GRN Lines")
            
            results["success"] = True
            
        except Exception as e:
            logger.exception("Error inserting invoice data: %s", e)
            results["errors"].append(str(e))
            raise
        
        return results
    
    async def _insert_po_header(self, po_header: POHeader) -> None:
        """Insert PO Header into database"""
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
    
    async def _insert_po_line(self, po_line: POLine) -> None:
        """Insert PO Line into database"""
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
    
    async def _insert_po_condition(self, po_condition: POCondition) -> None:
        """Insert PO Condition into database"""
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
    
    async def _insert_grn_header(self, grn_header: GRNHeader) -> None:
        """Insert GRN Header into database"""
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
    
    async def _insert_grn_line(self, grn_line: GRNLine) -> None:
        """Insert GRN Line into database"""
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
