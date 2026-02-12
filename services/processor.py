"""
Main invoice processor - orchestrates OCR data processing and database insertion
"""
from typing import Dict, Any
from pydantic import ValidationError

from models.ocr_input import OCRInput
from services.mapper import OCRMapper
from services.database import DatabaseService
from logs.log import logger


class InvoiceProcessor:
    """Main processor for invoice automation"""
    
    def __init__(self, run_query_func):
        """
        Initialize processor with database query function
        
        Args:
            run_query_func: Async function to execute database queries
        """
        self.mapper = OCRMapper()
        self.db_service = DatabaseService(run_query_func)
    
    async def process_invoice(self, ocr_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process OCR invoice data and insert into database
        
        Args:
            ocr_data: Dictionary containing 'dynamic' and 'static' invoice data
            
        Returns:
            Dictionary with processing results
            
        Raises:
            ValidationError: If OCR data validation fails
            Exception: If database insertion fails
        """
        try:
            # Step 1: Validate input data
            logger.info("Starting invoice processing")
            validated_input = self._validate_input(ocr_data)
            
            # Step 2: Map OCR data to database models
            logger.info("Mapping OCR data to database models")
            po_header, po_lines, po_conditions, grn_header, grn_lines = \
                self.mapper.map_to_database_models(validated_input)
            
            # Step 3: Insert into database
            logger.info("Inserting data into database")
            results = await self.db_service.insert_complete_invoice(
                po_header=po_header,
                po_lines=po_lines,
                po_conditions=po_conditions,
                grn_header=grn_header,
                grn_lines=grn_lines
            )
            
            # Step 4: Prepare response
            response = {
                "status": "success" if results["success"] else "failed",
                "message": "Invoice processed successfully" if results["success"] 
                          else "Invoice processing failed",
                "grn_number": grn_header.s_grn_number,
                "grn_id": grn_header.s_grn_id,
                "po_number": po_header.s_po_number if po_header else None,
                "details": {
                    "grn_header_id": results["grn_header_id"],
                    "grn_lines_inserted": results["grn_lines_count"],
                    "po_header_id": results["po_header_id"],
                    "po_lines_inserted": results["po_lines_count"],
                    "po_conditions_inserted": results["po_conditions_count"]
                },
                "errors": results["errors"]
            }
            
            logger.info(
                "Invoice processing completed: grn_number=%s, status=%s",
                grn_header.s_grn_number,
                response["status"]
            )
            
            return response
            
        except ValidationError as ve:
            logger.error("Validation error: %s", ve)
            return {
                "status": "failed",
                "message": "Invalid OCR data format",
                "errors": [str(ve)]
            }
        
        except Exception as e:
            logger.exception("Error processing invoice: %s", e)
            return {
                "status": "failed",
                "message": "Error processing invoice",
                "errors": [str(e)]
            }
    
    def _validate_input(self, ocr_data: Dict[str, Any]) -> OCRInput:
        """
        Validate OCR input data
        
        Args:
            ocr_data: Raw OCR data dictionary
            
        Returns:
            Validated OCRInput model
            
        Raises:
            ValidationError: If validation fails
        """
        try:
            return OCRInput(**ocr_data)
        except ValidationError as e:
            logger.error("OCR data validation failed: %s", e)
            raise
    
    async def process_batch_invoices(
        self, 
        invoice_list: list[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Process multiple invoices in batch
        
        Args:
            invoice_list: List of OCR data dictionaries
            
        Returns:
            Summary of batch processing
        """
        results = {
            "total": len(invoice_list),
            "successful": 0,
            "failed": 0,
            "invoices": []
        }
        
        for idx, invoice_data in enumerate(invoice_list, start=1):
            logger.info(f"Processing invoice {idx}/{len(invoice_list)}")
            
            result = await self.process_invoice(invoice_data)
            
            if result["status"] == "success":
                results["successful"] += 1
            else:
                results["failed"] += 1
            
            results["invoices"].append({
                "index": idx,
                "grn_number": result.get("grn_number"),
                "status": result["status"],
                "errors": result.get("errors", [])
            })
        
        logger.info(
            "Batch processing completed: total=%d, successful=%d, failed=%d",
            results["total"],
            results["successful"],
            results["failed"]
        )
        
        return results
