"""
FastAPI endpoints for invoice automation
"""
from fastapi import APIRouter, HTTPException, status
from typing import Dict, Any, List
from pydantic import BaseModel

from services.processor import InvoiceProcessor
from database.client import run_query
from logs.log import logger


# Router for invoice automation endpoints
router = APIRouter(prefix="/api/v1/invoice", tags=["Invoice Automation"])


# Request/Response models
class InvoiceProcessRequest(BaseModel):
    """Request model for processing single invoice"""
    dynamic: List[Dict[str, Any]]
    static: Dict[str, Any]


class BatchInvoiceProcessRequest(BaseModel):
    """Request model for processing multiple invoices"""
    invoices: List[InvoiceProcessRequest]


class InvoiceProcessResponse(BaseModel):
    """Response model for invoice processing"""
    status: str
    message: str
    grn_number: str = None
    grn_id: str = None
    po_number: str = None
    details: Dict[str, Any] = {}
    errors: List[str] = []


# Initialize processor (will be created per request)
def get_processor() -> InvoiceProcessor:
    """Get invoice processor instance"""
    return InvoiceProcessor(run_query)


@router.post(
    "/process",
    response_model=InvoiceProcessResponse,
    status_code=status.HTTP_200_OK,
    summary="Process single invoice from OCR data",
    description="Process OCR-extracted invoice data and insert into database tables"
)
async def process_invoice(request: InvoiceProcessRequest) -> InvoiceProcessResponse:
    """
    Process single invoice from OCR data
    
    Args:
        request: Invoice data with dynamic (lines) and static (header) fields
        
    Returns:
        Processing result with GRN number and insertion details
        
    Raises:
        HTTPException: If processing fails
    """
    try:
        logger.info("Received invoice processing request")
        
        processor = get_processor()
        result = await processor.process_invoice(request.dict())
        
        if result["status"] == "failed":
            logger.error("Invoice processing failed: %s", result.get("errors"))
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "message": result["message"],
                    "errors": result.get("errors", [])
                }
            )
        
        logger.info("Invoice processed successfully: grn=%s", result.get("grn_number"))
        return InvoiceProcessResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Unexpected error processing invoice: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/process/batch",
    status_code=status.HTTP_200_OK,
    summary="Process multiple invoices in batch",
    description="Process multiple OCR-extracted invoices in a single batch"
)
async def process_batch_invoices(
    request: BatchInvoiceProcessRequest
) -> Dict[str, Any]:
    """
    Process multiple invoices in batch
    
    Args:
        request: List of invoices to process
        
    Returns:
        Batch processing summary
    """
    try:
        logger.info("Received batch processing request for %d invoices", len(request.invoices))
        
        processor = get_processor()
        invoice_list = [invoice.dict() for invoice in request.invoices]
        
        results = await processor.process_batch_invoices(invoice_list)
        
        logger.info(
            "Batch processing completed: %d/%d successful",
            results["successful"],
            results["total"]
        )
        
        return results
        
    except Exception as e:
        logger.exception("Error in batch processing: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch processing error: {str(e)}"
        )


@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="Health check endpoint"
)
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint
    
    Returns:
        Status message
    """
    return {"status": "healthy", "service": "invoice-automation"}
