"""
Master Data Service - Insert all dependent entities before transactional data
Handles: Supplier, Sites, Items, Legal Entities, Cost Centers, etc.
"""
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, date

from logs.log import logger


class MasterDataService:
    """Service for inserting master data entities"""
    
    def __init__(self, run_query_func):
        """
        Initialize with the run_query function
        
        Args:
            run_query_func: Async function to execute database queries
        """
        self.run_query = run_query_func
        self.schema = "tenant_data"
        self._inserted_refs = {}  # Track what we've already inserted
    
    async def ensure_supplier(
        self,
        supplier_ref: UUID,
        supplier_name: str,
        supplier_gstn: Optional[str] = None
    ) -> UUID:
        """Ensure supplier exists, insert if not"""
        cache_key = f"supplier_{supplier_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_supplier 
            WHERE id = '{supplier_ref}' OR s_gstn = {self._format_string(supplier_gstn)}
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        # Insert new supplier
        insert_query = f"""
            INSERT INTO {self.schema}.s_supplier (
                id, is_deleted, created_at, updated_at,
                s_supplier_name, s_gstn, s_supplier_code,
                s_supplier_type, s_payment_terms, s_currency_id,
                s_tax_registration_number, s_active_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{supplier_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{self._escape_string(supplier_name)}',
                {self._format_string(supplier_gstn)},
                '{str(supplier_ref)[:20]}',
                'VENDOR', '0002', 'INR',
                {self._format_string(supplier_gstn)},
                true,
                '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = supplier_ref
        logger.info(f"Inserted supplier: {supplier_name}")
        return supplier_ref
    
    async def ensure_supplier_site(
        self,
        site_ref: UUID,
        supplier_ref: UUID,
        address: Optional[str] = None,
        gstn: Optional[str] = None
    ) -> UUID:
        """Ensure supplier site exists, insert if not"""
        cache_key = f"supplier_site_{site_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_supplier_site
            WHERE id = '{site_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        # Insert new supplier site
        insert_query = f"""
            INSERT INTO {self.schema}.s_supplier_site (
                id, is_deleted, created_at, updated_at,
                s_supplier_ref, s_site_name, s_address_line1,
                s_gstn, s_active_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{site_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{supplier_ref}',
                'Primary Site',
                {self._format_string(address or 'Address Not Provided')},
                {self._format_string(gstn)},
                true,
                '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = site_ref
        logger.info(f"Inserted supplier site: {site_ref}")
        return site_ref
    
    async def ensure_legal_entity(
        self,
        entity_ref: UUID,
        gstn: Optional[str] = None
    ) -> UUID:
        """Ensure legal entity exists, insert if not"""
        cache_key = f"legal_entity_{entity_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_legal_entity
            WHERE id = '{entity_ref}' OR s_gstn = {self._format_string(gstn)}
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        # Insert new legal entity
        insert_query = f"""
            INSERT INTO {self.schema}.s_legal_entity (
                id, is_deleted, created_at, updated_at,
                s_legal_entity_name, s_legal_entity_code,
                s_gstn, s_pan, s_currency_id,
                s_active_flag, s_effective_from, s_effective_to
            ) VALUES (
                '{entity_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                'Default Legal Entity',
                '{str(entity_ref)[:20]}',
                {self._format_string(gstn)},
                NULL, 'INR',
                true, '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = entity_ref
        logger.info(f"Inserted legal entity: {entity_ref}")
        return entity_ref
    
    async def ensure_legal_entity_site(
        self,
        site_ref: UUID,
        entity_ref: UUID,
        address: Optional[str] = None,
        gstn: Optional[str] = None
    ) -> UUID:
        """Ensure legal entity site exists, insert if not"""
        cache_key = f"legal_entity_site_{site_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_legal_entity_site
            WHERE id = '{site_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        # Insert new legal entity site
        insert_query = f"""
            INSERT INTO {self.schema}.s_legal_entity_site (
                id, is_deleted, created_at, updated_at,
                s_legal_entity_ref, s_site_name, s_address_line1,
                s_gstn, s_active_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{site_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{entity_ref}',
                'Primary Site',
                {self._format_string(address or 'Address Not Provided')},
                {self._format_string(gstn)},
                true,
                '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = site_ref
        logger.info(f"Inserted legal entity site: {site_ref}")
        return site_ref
    
    async def ensure_item(
        self,
        item_ref: UUID,
        description: str,
        hsn_code: str,
        uom: str = "EA"
    ) -> UUID:
        """Ensure item exists, insert if not"""
        cache_key = f"item_{item_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_item
            WHERE id = '{item_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        # Insert new item
        insert_query = f"""
            INSERT INTO {self.schema}.s_item (
                id, is_deleted, created_at, updated_at,
                s_item_code, s_item_description, s_hsn_code,
                s_base_uom_id, s_item_type, s_active_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{item_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(item_ref)[:50]}',
                '{self._escape_string(description[:255])}',
                '{hsn_code}',
                '{uom}', 'MATERIAL', true,
                '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = item_ref
        logger.info(f"Inserted item: {description[:50]}")
        return item_ref
    
    async def ensure_cost_center(self, cost_center_ref: UUID) -> UUID:
        """Ensure cost center exists, insert if not"""
        cache_key = f"cost_center_{cost_center_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_cost_center
            WHERE id = '{cost_center_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_cost_center (
                id, is_deleted, created_at, updated_at,
                s_cost_center_code, s_cost_center_name,
                s_active_flag, s_effective_from, s_effective_to
            ) VALUES (
                '{cost_center_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(cost_center_ref)[:20]}',
                'Default Cost Center',
                true, '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = cost_center_ref
        logger.info(f"Inserted cost center: {cost_center_ref}")
        return cost_center_ref
    
    async def ensure_profit_center(self, profit_center_ref: UUID) -> UUID:
        """Ensure profit center exists, insert if not"""
        cache_key = f"profit_center_{profit_center_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_profit_center
            WHERE id = '{profit_center_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_profit_center (
                id, is_deleted, created_at, updated_at,
                s_profit_center_code, s_profit_center_name,
                s_active_flag, s_effective_from, s_effective_to
            ) VALUES (
                '{profit_center_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(profit_center_ref)[:20]}',
                'Default Profit Center',
                true, '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = profit_center_ref
        logger.info(f"Inserted profit center: {profit_center_ref}")
        return profit_center_ref
    
    async def ensure_project(self, project_ref: UUID) -> UUID:
        """Ensure project exists, insert if not"""
        cache_key = f"project_{project_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_project
            WHERE id = '{project_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_project (
                id, is_deleted, created_at, updated_at,
                s_project_code, s_project_name,
                s_active_flag, s_effective_from, s_effective_to
            ) VALUES (
                '{project_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(project_ref)[:20]}',
                'Default Project',
                true, '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = project_ref
        logger.info(f"Inserted project: {project_ref}")
        return project_ref
    
    async def ensure_plant(self, plant_ref: UUID) -> UUID:
        """Ensure plant exists, insert if not"""
        cache_key = f"plant_{plant_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_plant
            WHERE id = '{plant_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_plant (
                id, is_deleted, created_at, updated_at,
                s_plant_code, s_plant_name,
                s_active_flag, s_effective_from, s_effective_to
            ) VALUES (
                '{plant_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(plant_ref)[:20]}',
                'Default Plant',
                true, '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = plant_ref
        logger.info(f"Inserted plant: {plant_ref}")
        return plant_ref
    
    async def ensure_gl_account(self, gl_account_ref: UUID) -> UUID:
        """Ensure GL account exists, insert if not"""
        cache_key = f"gl_account_{gl_account_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_gl_account
            WHERE id = '{gl_account_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_gl_account (
                id, is_deleted, created_at, updated_at,
                s_gl_account_code, s_gl_account_name,
                s_account_type, s_active_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{gl_account_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(gl_account_ref)[:20]}',
                'Inventory Account',
                'ASSET', true,
                '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = gl_account_ref
        logger.info(f"Inserted GL account: {gl_account_ref}")
        return gl_account_ref
    
    async def ensure_tax_rate(self, tax_rate_ref: UUID, rate: float = 18.0) -> UUID:
        """Ensure tax rate exists, insert if not"""
        cache_key = f"tax_rate_{tax_rate_ref}"
        if cache_key in self._inserted_refs:
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_tax_rate
            WHERE id = '{tax_rate_ref}'
            LIMIT 1;
        """
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            return result[0]['id']
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_tax_rate (
                id, is_deleted, created_at, updated_at,
                s_tax_code, s_tax_rate, s_tax_type,
                s_active_flag, s_effective_from, s_effective_to
            ) VALUES (
                '{tax_rate_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                'GST{int(rate)}',
                {rate}, 'GST',
                true, '{date.today()}', NULL
            );
        """
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = tax_rate_ref
        logger.info(f"Inserted tax rate: {rate}%")
        return tax_rate_ref
    
    def _format_string(self, s: Optional[str]) -> str:
        """Format string for PostgreSQL"""
        if s is None or s == "":
            return "NULL"
        return f"'{self._escape_string(s)}'"
    
    def _escape_string(self, s: str) -> str:
        """Escape single quotes in strings"""
        return s.replace("'", "''")
    
    def clear_cache(self):
        """Clear insertion cache"""
        self._inserted_refs.clear()
