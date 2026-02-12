"""
Master Data Service - Insert all dependent entities before transactional data
FIXED: All SQL queries now use correct PostgreSQL syntax
VERIFIED: All column names match actual database schema from dump.txt
"""
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, date

from logs.log import logger


class MasterDataService:
    """Service for inserting master data entities with CORRECT SQL syntax"""
    
    def __init__(self, run_query_func):
        """
        Initialize with the run_query function
        
        Args:
            run_query_func: Async function to execute database queries
        """
        self.run_query = run_query_func
        self.schema = "tenant_data"
        self._inserted_refs = {}  # Track what we've already inserted
        
        # Cache for reference data (country, state) to avoid repeated queries
        self._country_cache = {}
        self._state_cache = {}
    
    async def _get_default_country_id(self) -> str:
        """Get default country ID (India)"""
        if 'IN' in self._country_cache:
            logger.debug(f"Using cached country ID for 'IN': {self._country_cache['IN']}")
            return self._country_cache['IN']
        
        # FIXED: Correct SQL syntax - don't prefix columns with schema name
        query = f"""
            SELECT s_country_id 
            FROM {self.schema}.s_country 
            WHERE s_country_code = 'IN'
            LIMIT 1;
        """
        logger.debug(f"Fetching default country ID with query: {query.strip()}")
        result = await self.run_query(query)
        
        if result:
            country_id = result[0]['s_country_id']
            self._country_cache['IN'] = country_id
            logger.info(f"✓ Found country ID for 'IN': {country_id}")
            return country_id
        
        # If no country found, return a placeholder - this should not happen in production
        logger.warning("⚠️ No country found for code 'IN', using placeholder 'IN'")
        return "IN"
    
    async def _get_default_state_id(self) -> str:
        """Get default state ID (Delhi)"""
        if 'DL' in self._state_cache:
            logger.debug(f"Using cached state ID for 'DL': {self._state_cache['DL']}")
            return self._state_cache['DL']
        
        # FIXED: Correct SQL syntax - don't prefix columns with schema name
        query = f"""
            SELECT s_state_id 
            FROM {self.schema}.s_state 
            WHERE s_state_code = 'DL'
            LIMIT 1;
        """
        logger.debug(f"Fetching default state ID with query: {query.strip()}")
        result = await self.run_query(query)
        
        if result:
            state_id = result[0]['s_state_id']
            self._state_cache['DL'] = state_id
            logger.info(f"✓ Found state ID for 'DL': {state_id}")
            return state_id
        
        # If no state found, return a placeholder
        logger.warning("⚠️ No state found for code 'DL', using placeholder 'DL'")
        return "DL"
    
    async def ensure_supplier(
        self,
        supplier_ref: UUID,
        supplier_name: str,
        supplier_gstn: Optional[str] = None
    ) -> UUID:
        """Ensure supplier exists, insert if not"""
        cache_key = f"supplier_{supplier_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Supplier {supplier_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        # Check if exists by ID only (no GSTN in supplier table)
        check_query = f"""
            SELECT id FROM {self.schema}.s_supplier 
            WHERE id = '{supplier_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if supplier exists: {supplier_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.info(f"✓ Supplier already exists: {supplier_ref}")
            return result[0]['id']
        
        # Extract PAN from GSTIN (first 10 chars) or use default
        pan_number = supplier_gstn[:10] if supplier_gstn and len(supplier_gstn) >= 10 else "AAAPZ1234C"
        
        # Insert new supplier - VERIFIED column names from schema
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
            );
        """
        logger.debug(f"Inserting supplier: {supplier_name}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = supplier_ref
        logger.info(f"✓ Inserted supplier: {supplier_name} (PAN: {pan_number})")
        return supplier_ref
    
    async def ensure_supplier_site(
        self,
        site_ref: UUID,
        supplier_ref: UUID,
        address: Optional[str] = None,
        gstin: Optional[str] = None  # FIXED: parameter name is 'gstin' not 'gstn'
    ) -> UUID:
        """Ensure supplier site exists, insert if not"""
        cache_key = f"supplier_site_{site_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Supplier site {site_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_supplier_site
            WHERE id = '{site_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if supplier site exists: {site_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.info(f"✓ Supplier site already exists: {site_ref}")
            return result[0]['id']
        
        # Get default country and state
        logger.debug("Fetching default country and state...")
        country_id = await self._get_default_country_id()
        state_id = await self._get_default_state_id()
        
        # Insert new supplier site - VERIFIED column names from schema
        # Column is s_gstin (not s_gstn!)
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
            );
        """
        logger.debug(f"Inserting supplier site with GSTIN: {gstin}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = site_ref
        logger.info(f"✓ Inserted supplier site: {site_ref} (GSTIN: {gstin})")
        return site_ref
    
    async def ensure_legal_entity(
        self,
        entity_ref: UUID,
        gstin: Optional[str] = None  # FIXED: parameter name is 'gstin' not 'gstn'
    ) -> UUID:
        """Ensure legal entity exists, insert if not"""
        cache_key = f"legal_entity_{entity_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Legal entity {entity_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_legal_entity
            WHERE id = '{entity_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if legal entity exists: {entity_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.info(f"✓ Legal entity already exists: {entity_ref}")
            return result[0]['id']
        
        # Extract PAN from GSTIN or use default
        pan = gstin[:10] if gstin and len(gstin) >= 10 else "AAAPZ9999C"
        
        # Insert new legal entity - VERIFIED column names from schema
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
            );
        """
        logger.debug(f"Inserting legal entity with PAN: {pan}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = entity_ref
        logger.info(f"✓ Inserted legal entity: {entity_ref} (PAN: {pan})")
        return entity_ref
    
    async def ensure_legal_entity_site(
        self,
        site_ref: UUID,
        entity_ref: UUID,
        address: Optional[str] = None,
        gstin: Optional[str] = None  # FIXED: parameter name is 'gstin' not 'gstn'
    ) -> UUID:
        """Ensure legal entity site exists, insert if not"""
        cache_key = f"legal_entity_site_{site_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Legal entity site {site_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_legal_entity_site
            WHERE id = '{site_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if legal entity site exists: {site_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.info(f"✓ Legal entity site already exists: {site_ref}")
            return result[0]['id']
        
        # Get default country and state
        logger.debug("Fetching default country and state...")
        country_id = await self._get_default_country_id()
        state_id = await self._get_default_state_id()
        
        # Insert new legal entity site - VERIFIED column names from schema
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
            );
        """
        logger.debug(f"Inserting legal entity site with GSTIN: {gstin}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = site_ref
        logger.info(f"✓ Inserted legal entity site: {site_ref} (GSTIN: {gstin})")
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
            logger.debug(f"Item {item_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        # Check if exists
        check_query = f"""
            SELECT id FROM {self.schema}.s_item
            WHERE id = '{item_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if item exists: {item_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.debug(f"✓ Item already exists: {description[:50]}")
            return result[0]['id']
        
        # Insert new item - VERIFIED column names from schema
        # s_item_code, s_item_name (not s_item_description!)
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
            );
        """
        logger.debug(f"Inserting item: {description[:50]}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = item_ref
        logger.debug(f"✓ Inserted item: {description[:50]} (HSN: {hsn_code})")
        return item_ref
    
    async def ensure_cost_center(self, cost_center_ref: UUID) -> UUID:
        """Ensure cost center exists, insert if not"""
        cache_key = f"cost_center_{cost_center_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Cost center {cost_center_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_cost_center
            WHERE id = '{cost_center_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if cost center exists: {cost_center_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.debug(f"✓ Cost center already exists: {cost_center_ref}")
            return result[0]['id']
        
        # VERIFIED column names from schema
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
            );
        """
        logger.debug(f"Inserting cost center: {cost_center_ref}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = cost_center_ref
        logger.debug(f"✓ Inserted cost center: {cost_center_ref}")
        return cost_center_ref
    
    async def ensure_profit_center(self, profit_center_ref: UUID) -> UUID:
        """Ensure profit center exists, insert if not"""
        cache_key = f"profit_center_{profit_center_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Profit center {profit_center_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_profit_center
            WHERE id = '{profit_center_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if profit center exists: {profit_center_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.debug(f"✓ Profit center already exists: {profit_center_ref}")
            return result[0]['id']
        
        # VERIFIED column names from schema
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
            );
        """
        logger.debug(f"Inserting profit center: {profit_center_ref}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = profit_center_ref
        logger.debug(f"✓ Inserted profit center: {profit_center_ref}")
        return profit_center_ref
    
    async def ensure_project(self, project_ref: UUID) -> UUID:
        """Ensure project exists, insert if not
        NOTE: Table is s_project_wbs not s_project!"""
        cache_key = f"project_{project_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Project {project_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_project_wbs
            WHERE id = '{project_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if project exists: {project_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.debug(f"✓ Project already exists: {project_ref}")
            return result[0]['id']
        
        # VERIFIED: Table is s_project_wbs, VERIFIED column names
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
            );
        """
        logger.debug(f"Inserting project: {project_ref}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = project_ref
        logger.debug(f"✓ Inserted project: {project_ref}")
        return project_ref
    
    async def ensure_plant(self, plant_ref: UUID) -> UUID:
        """Ensure plant exists, insert if not"""
        cache_key = f"plant_{plant_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Plant {plant_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_plant
            WHERE id = '{plant_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if plant exists: {plant_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.debug(f"✓ Plant already exists: {plant_ref}")
            return result[0]['id']
        
        # VERIFIED column names from schema
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
            );
        """
        logger.debug(f"Inserting plant: {plant_ref}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = plant_ref
        logger.debug(f"✓ Inserted plant: {plant_ref}")
        return plant_ref
    
    async def ensure_gl_account(self, gl_account_ref: UUID) -> UUID:
        """Ensure GL account exists, insert if not"""
        cache_key = f"gl_account_{gl_account_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"GL account {gl_account_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_gl_account
            WHERE id = '{gl_account_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if GL account exists: {gl_account_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.debug(f"✓ GL account already exists: {gl_account_ref}")
            return result[0]['id']
        
        # VERIFIED column names from schema
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
            );
        """
        logger.debug(f"Inserting GL account: {gl_account_ref}")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = gl_account_ref
        logger.debug(f"✓ Inserted GL account: {gl_account_ref}")
        return gl_account_ref
    
    async def ensure_tax_rate(self, tax_rate_ref: UUID, rate: float = 18.0) -> UUID:
        """Ensure tax rate exists, insert if not"""
        cache_key = f"tax_rate_{tax_rate_ref}"
        if cache_key in self._inserted_refs:
            logger.debug(f"Tax rate {tax_rate_ref} already in cache")
            return self._inserted_refs[cache_key]
        
        check_query = f"""
            SELECT id FROM {self.schema}.s_tax_rate
            WHERE id = '{tax_rate_ref}'
            LIMIT 1;
        """
        logger.debug(f"Checking if tax rate exists: {tax_rate_ref}")
        result = await self.run_query(check_query)
        
        if result:
            self._inserted_refs[cache_key] = result[0]['id']
            logger.debug(f"✓ Tax rate already exists: {tax_rate_ref}")
            return result[0]['id']
        
        # VERIFIED column names from schema
        insert_query = f"""
            INSERT INTO {self.schema}.s_tax_rate (
                id, is_deleted, created_at, updated_at,
                s_tax_rate_id, s_tax_rate_name,
                s_effective_from, s_effective_to
            ) VALUES (
                '{tax_rate_ref}', false,
                '{datetime.now()}', '{datetime.now()}',
                '{str(tax_rate_ref)[:36]}',
                'GST{int(rate)}%',
                '{date.today()}', NULL
            );
        """
        logger.debug(f"Inserting tax rate: {rate}%")
        await self.run_query(insert_query)
        self._inserted_refs[cache_key] = tax_rate_ref
        logger.debug(f"✓ Inserted tax rate: {rate}%")
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
        self._country_cache.clear()
        self._state_cache.clear()
        logger.info("✓ Master data cache cleared")
