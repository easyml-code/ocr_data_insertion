"""
Example usage of the invoice automation system
"""
import asyncio
import json
from services.processor import InvoiceProcessor
from database.client import run_query


# Example OCR data (same as provided in requirements)
EXAMPLE_OCR_DATA = {
    "dynamic": [
        {
            "Invoice Lines/Description": "Dell UltraSharp 27-inch QHD IPS Monitor, 165Hz, USB-C",
            "Quantity": "1",
            "Line Amount": "17999.00",
            "Unit Price": "179999.00",
            "HSN Number": "852851",
            "igst_rate": "18",
            "sgst_rate": "",
            "cgst_rate": "",
            "utgst_rate": "",
            "PO Number": "9500877232",
            "line_no": "",
            "unit": ""
        }
    ],
    "static": {
        "Invoice Date": ["15-Aug-2025"],
        "Invoice Currency": ["INR"],
        "supplier_city": ["Hyderabad"],
        "supplier_state": ["INDIA"],
        "Total Invoice Amount": ["21238.82"],
        "Invoice Tax Amount": ["3239.82"],
        "subtotal": ["17999.00"],
        "delivery_location": ["Mumbai, Maharashtra- 400028"],
        "Invoice information": [""],
        "account_number": ["054205001505"],
        "bill_to_address": ["252, Veer Savarkar Road, Shivaji Park, Mumbai, Maharashtra- 400028"],
        "ship_to_address": ["D-9, Banjara Hills, Hyderabad"],
        "shipping_amount": [""],
        "hsn_code": ["852851"],
        "Supplier GSTN": ["36ABCDE1234F1Z5"],
        "Location GSTN": ["36ABCDE1234F1Z5"],
        "Supplier Name": ["Orion Corp"],
        "irn": ["36ABCDE1234F1Z5"],
        "Invoice No": ["2024-29/09/007"],
        "CGST": [""],
        "SGST": [""],
        "IGST": ["18%"],
        "PO Number": ["9500877232"],
        "supplier_address": ["D-9, Banjara Hills, Hyderabad"],
        "consumer_number": [""],
        "customer_address": ["252, Veer Savarkar Road, Shivaji Park, Mumbai, Maharashtra- 400028"],
        "file_name": ["invoice_2024-29_09_007.pdf"],
        "gst_number": [""],
        "due_date": [""],
        "Applicable Tax": [""],
        "currency": [""]
    }
}


async def process_single_invoice_example():
    """Example: Process a single invoice"""
    print("=" * 80)
    print("EXAMPLE 1: Processing Single Invoice")
    print("=" * 80)
    
    # Create processor
    processor = InvoiceProcessor(run_query)
    
    # Process invoice
    result = await processor.process_invoice(EXAMPLE_OCR_DATA)
    
    # Print results
    print(json.dumps(result, indent=2, default=str))
    
    return result


async def process_batch_invoices_example():
    """Example: Process multiple invoices in batch"""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Processing Batch Invoices")
    print("=" * 80)
    
    # Create multiple invoice entries
    invoices = [EXAMPLE_OCR_DATA] * 3  # Process same invoice 3 times for demo
    
    # Create processor
    processor = InvoiceProcessor(run_query)
    
    # Process batch
    result = await processor.process_batch_invoices(invoices)
    
    # Print results
    print(json.dumps(result, indent=2, default=str))
    
    return result


async def standalone_usage_example():
    """
    Example: Using individual components standalone
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Standalone Component Usage")
    print("=" * 80)
    
    from models.ocr_input import OCRInput
    from services.mapper import OCRMapper
    from services.database import DatabaseService
    
    # Step 1: Validate input
    print("\nStep 1: Validating OCR input...")
    validated_input = OCRInput(**EXAMPLE_OCR_DATA)
    print(f"  ✓ Validated {len(validated_input.dynamic)} line items")
    
    # Step 2: Map to database models
    print("\nStep 2: Mapping to database models...")
    mapper = OCRMapper()
    po_header, po_lines, po_conditions, grn_header, grn_lines = \
        mapper.map_to_database_models(validated_input)
    
    print(f"  ✓ Generated GRN Number: {grn_header.s_grn_number}")
    print(f"  ✓ Generated GRN ID: {grn_header.s_grn_id}")
    if po_header:
        print(f"  ✓ Generated PO Number: {po_header.s_po_number}")
    print(f"  ✓ Mapped {len(grn_lines)} GRN lines")
    print(f"  ✓ Mapped {len(po_lines)} PO lines")
    print(f"  ✓ Mapped {len(po_conditions)} PO conditions")
    
    # Step 3: Insert into database (commented out to avoid actual DB operations)
    print("\nStep 3: Database insertion...")
    print("  (Skipped in example - set up database connection first)")
    
    # Uncomment below to actually insert
    # db_service = DatabaseService(run_query)
    # result = await db_service.insert_complete_invoice(
    #     po_header, po_lines, po_conditions, grn_header, grn_lines
    # )
    # print(f"  ✓ Inserted successfully: {result}")


async def main():
    """Run all examples"""
    print("\n" + "=" * 80)
    print("INVOICE AUTOMATION SYSTEM - USAGE EXAMPLES")
    print("=" * 80)
    
    try:
        # Example 1: Single invoice processing
        await process_single_invoice_example()
        
        # Example 2: Batch processing
        # await process_batch_invoices_example()
        
        # Example 3: Standalone component usage
        await standalone_usage_example()
        
        print("\n" + "=" * 80)
        print("Examples completed successfully!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run examples
    asyncio.run(main())
