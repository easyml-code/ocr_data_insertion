"""
Master Data Service - Enterprise Solution
FIXED: Removed all caching - uses proper INSERT ON CONFLICT pattern
FIXED: Tax rate names now composite (e.g., IGST_18, CGST_SGST_18)
VERIFIED: All column names match actual database schema from dump.txt
"""
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, date

from logs.log import logger


class MasterDataService:
    """Service for inserting master data entities using enterprise best practices"""
    
    def __init__(self, run_query_func):
        """
        Initialize with the run_query function
        
        Args:
            run_query_func: Async function to execute database queries
        """
        self.run_query = run_query_func
        self.schema = "tenant_data"
    
    async def _get_country_id_by_code(self, country_code: str) -> Optional[str]:
        """Get country ID by country code"""
        query = f"""
            SELECT s_country_id 
            FROM {self.schema}.s_country 
            WHERE s_country_code = '{country_code}'
            LIMIT 1;
        """
        result = await self.run_query(query)
        
        if result:
            country_id = result[0]['s_country_id']
            logger.info(f"✓ Found country ID for '{country_code}': {country_id}")
            return country_id
        
        logger.warning(f"⚠️ No country found for code '{country_code}'")
        return None
    
    async def _get_state_id_by_code(self, state_code: str) -> Optional[str]:
        """Get state ID by state code"""
        query = f"""
            SELECT s_state_id 
            FROM {self.schema}.s_state 
            WHERE s_state_code = '{state_code}'
            LIMIT 1;
        """
        result = await self.run_query(query)
        
        if result:
            state_id = result[0]['s_state_id']
            logger.info(f"✓ Found state ID for '{state_code}': {state_id}")
            return state_id
        
        logger.warning(f"⚠️ No state found for code '{state_code}'")
        return None
    
    async def ensure_supplier(
        self,
        supplier_ref: UUID,
        supplier_name: str,
        supplier_gstn: Optional[str] = None
    ) -> UUID:
        """Ensure supplier exists using INSERT ON CONFLICT"""
        
        # Extract PAN from GSTIN (first 10 chars) or use default
        pan_number = supplier_gstn[:10] if supplier_gstn and len(supplier_gstn) >= 10 else "AAAPZ1234C"
        
        # INSERT ON CONFLICT DO NOTHING - enterprise pattern
        insert_query = f"""
            INSERT INTO {self.schema}.s_supplier (
                id, is_deleted, created_at, updated_at,
                s_supplier_id, s_supplier_code, s_legal_name, s_pan_number,
                s_supplier_type, s_msme_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{supplier_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(supplier_ref)[:36]}',
                '{str(supplier_ref)[:20]}',
                '{self._escape_string(supplier_name[:255])}',
                '{pan_number}',
                'COMPANY', false,
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.info(f"✓ Supplier ensured: {supplier_name} (PAN: {pan_number})")
        return supplier_ref
    
    async def ensure_supplier_site(
        self,
        site_ref: UUID,
        supplier_ref: UUID,
        address: Optional[str] = None,
        gstin: Optional[str] = None
    ) -> UUID:
        """Ensure supplier site exists using INSERT ON CONFLICT"""
        
        # Get default country and state
        country_id = await self._get_country_id_by_code('IN') or 'COUNTRY31'
        state_id = await self._get_state_id_by_code('DL') or 'STATE7'
        
        # INSERT ON CONFLICT DO NOTHING
        insert_query = f"""
            INSERT INTO {self.schema}.s_supplier_site (
                id, is_deleted, created_at, updated_at,
                s_supplier_site_id, s_gstin, s_country_id, s_state_id,
                s_building_name, s_floor_unit, s_city, s_pin_code,
                s_supplier_ref, s_sez_flag, s_default_dispatch_flag, s_default_billing_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{site_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(site_ref)[:36]}',
                {self._format_string(gstin)},
                '{country_id}',
                '{state_id}',
                'Default Building',
                'Ground Floor',
                'Delhi',
                '110001',
                '{supplier_ref}',
                false, true, true,
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.info(f"✓ Supplier site ensured: {site_ref} (GSTIN: {gstin})")
        return site_ref
    
    async def ensure_legal_entity(
        self,
        entity_ref: UUID,
        gstin: Optional[str] = None
    ) -> UUID:
        """Ensure legal entity exists using INSERT ON CONFLICT"""
        
        # Extract PAN from GSTIN or use default
        pan = gstin[:10] if gstin and len(gstin) >= 10 else "AAAPZ9999C"
        
        # INSERT ON CONFLICT DO NOTHING
        insert_query = f"""
            INSERT INTO {self.schema}.s_legal_entity (
                id, is_deleted, created_at, updated_at,
                s_legal_entity_id, s_legal_entity_name, s_legal_entity_pan,
                s_effective_from, s_effective_to
            ) VALUES (
                '{entity_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(entity_ref)[:36]}',
                'Default Legal Entity',
                '{pan}',
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.info(f"✓ Legal entity ensured: {entity_ref} (PAN: {pan})")
        return entity_ref
    
    async def ensure_legal_entity_site(
        self,
        site_ref: UUID,
        entity_ref: UUID,
        address: Optional[str] = None,
        gstin: Optional[str] = None
    ) -> UUID:
        """Ensure legal entity site exists using INSERT ON CONFLICT"""
        
        # Get default country and state
        country_id = await self._get_country_id_by_code('IN') or 'COUNTRY31'
        state_id = await self._get_state_id_by_code('DL') or 'STATE7'
        
        # INSERT ON CONFLICT DO NOTHING
        insert_query = f"""
            INSERT INTO {self.schema}.s_legal_entity_site (
                id, is_deleted, created_at, updated_at,
                s_legal_entity_site_id, s_gstin, s_country_id, s_state_id,
                s_building_name, s_floor_unit, s_city, s_pin_code,
                s_legal_entity_ref, s_sez_flag, s_default_shipping_flag, s_default_billing_flag,
                s_effective_from, s_effective_to
            ) VALUES (
                '{site_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(site_ref)[:36]}',
                {self._format_string(gstin)},
                '{country_id}',
                '{state_id}',
                'Default Building',
                'Ground Floor',
                'Delhi',
                '110001',
                '{entity_ref}',
                false, true, true,
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.info(f"✓ Legal entity site ensured: {site_ref} (GSTIN: {gstin})")
        return site_ref
    
    async def ensure_item(
        self,
        item_ref: UUID,
        description: str,
        hsn_code: str,
        uom: str = "EA"
    ) -> UUID:
        """Ensure item exists using INSERT ON CONFLICT"""
        
        # INSERT ON CONFLICT DO NOTHING
        insert_query = f"""
            INSERT INTO {self.schema}.s_item (
                id, is_deleted, created_at, updated_at,
                s_item_id, s_item_code, s_item_name, s_item_category,
                s_hsn_id, s_uom_id,
                s_effective_from, s_effective_to
            ) VALUES (
                '{item_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(item_ref)[:36]}',
                '{str(item_ref)[:50]}',
                '{self._escape_string(description[:255])}',
                'MATERIAL',
                '{hsn_code}',
                '{uom}',
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.debug(f"✓ Item ensured: {description[:50]} (HSN: {hsn_code})")
        return item_ref
    
    async def ensure_cost_center(self, cost_center_ref: UUID) -> UUID:
        """Ensure cost center exists using INSERT ON CONFLICT"""
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_cost_center (
                id, is_deleted, created_at, updated_at,
                s_cost_center_id, s_cost_center_code, s_cost_center_description,
                s_effective_from, s_effective_to
            ) VALUES (
                '{cost_center_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(cost_center_ref)[:36]}',
                '{str(cost_center_ref)[:20]}',
                'Default Cost Center',
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.debug(f"✓ Cost center ensured: {cost_center_ref}")
        return cost_center_ref
    
    async def ensure_profit_center(self, profit_center_ref: UUID) -> UUID:
        """Ensure profit center exists using INSERT ON CONFLICT"""
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_profit_center (
                id, is_deleted, created_at, updated_at,
                s_profit_center_id, s_profit_center_code,
                s_effective_from, s_effective_to
            ) VALUES (
                '{profit_center_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(profit_center_ref)[:36]}',
                '{str(profit_center_ref)[:20]}',
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.debug(f"✓ Profit center ensured: {profit_center_ref}")
        return profit_center_ref
    
    async def ensure_project(self, project_ref: UUID) -> UUID:
        """Ensure project exists using INSERT ON CONFLICT
        NOTE: Table is s_project_wbs not s_project!"""
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_project_wbs (
                id, is_deleted, created_at, updated_at,
                s_project_id, s_project_code,
                s_effective_from, s_effective_to
            ) VALUES (
                '{project_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(project_ref)[:36]}',
                '{str(project_ref)[:20]}',
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.debug(f"✓ Project ensured: {project_ref}")
        return project_ref
    
    async def ensure_plant(self, plant_ref: UUID) -> UUID:
        """Ensure plant exists using INSERT ON CONFLICT"""
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_plant (
                id, is_deleted, created_at, updated_at,
                s_plant_id, s_plant_code, s_plant_description,
                s_effective_from, s_effective_to
            ) VALUES (
                '{plant_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(plant_ref)[:36]}',
                '{str(plant_ref)[:20]}',
                'Default Plant',
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.debug(f"✓ Plant ensured: {plant_ref}")
        return plant_ref
    
    async def ensure_gl_account(self, gl_account_ref: UUID) -> UUID:
        """Ensure GL account exists using INSERT ON CONFLICT"""
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_gl_account (
                id, is_deleted, created_at, updated_at,
                s_gl_account_id, s_gl_account_code, s_gl_account_description,
                s_gl_account_type,
                s_effective_from, s_effective_to
            ) VALUES (
                '{gl_account_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(gl_account_ref)[:36]}',
                '{str(gl_account_ref)[:20]}',
                'Inventory Account',
                'ASSET',
                '{date.today()}', NULL
            )
            ON CONFLICT (id) DO NOTHING;
        """
        
        await self.run_query(insert_query)
        logger.debug(f"✓ GL account ensured: {gl_account_ref}")
        return gl_account_ref
    
    async def ensure_tax_rate(self, tax_rate_name: str) -> UUID:
        """
        Ensure tax rate exists by NAME (unique constraint)
        Returns the UUID of the tax rate (existing or newly created)
        
        ENTERPRISE PATTERN: 
        1. Try to find by name (unique constraint)
        2. If not found, insert with new UUID
        3. If duplicate name error, retry SELECT
        """
        
        # Step 1: Try to find existing by name
        select_query = f"""
            SELECT id 
            FROM {self.schema}.s_tax_rate 
            WHERE s_tax_rate_name = '{self._escape_string(tax_rate_name)}'
            LIMIT 1;
        """
        
        result = await self.run_query(select_query)
        
        if result:
            tax_rate_id = result[0]['id']
            logger.debug(f"✓ Tax rate already exists: {tax_rate_name} -> {tax_rate_id}")
            return UUID(tax_rate_id)
        
        # Step 2: Not found, insert with new UUID
        from uuid import uuid4
        new_tax_rate_id = uuid4()
        
        insert_query = f"""
            INSERT INTO {self.schema}.s_tax_rate (
                id, is_deleted, created_at, updated_at,
                s_tax_rate_id, s_tax_rate_name,
                s_effective_from, s_effective_to
            ) VALUES (
                '{new_tax_rate_id}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(new_tax_rate_id)[:36]}',
                '{self._escape_string(tax_rate_name)}',
                '{date.today()}', NULL
            );
        """
        
        try:
            await self.run_query(insert_query)
            logger.info(f"✓ Tax rate created: {tax_rate_name} -> {new_tax_rate_id}")
            return new_tax_rate_id
        except Exception as e:
            # If unique constraint violation (someone inserted between our SELECT and INSERT)
            # Retry the SELECT
            error_msg = str(e).lower()
            if 'duplicate' in error_msg or 'unique' in error_msg:
                logger.info(f"Tax rate '{tax_rate_name}' was created concurrently, fetching ID...")
                result = await self.run_query(select_query)
                if result:
                    tax_rate_id = result[0]['id']
                    logger.info(f"✓ Tax rate found after retry: {tax_rate_name} -> {tax_rate_id}")
                    return UUID(tax_rate_id)
            
            # If it's some other error, re-raise
            logger.error(f"Failed to ensure tax rate '{tax_rate_name}': {e}")
            raise
    
    def _format_string(self, s: Optional[str]) -> str:
        """Format string for PostgreSQL"""
        if s is None or s == "":
            return "NULL"
        return f"'{self._escape_string(s)}'"
    
    def _escape_string(self, s: str) -> str:
        """Escape single quotes in strings"""
        return s.replace("'", "''")
