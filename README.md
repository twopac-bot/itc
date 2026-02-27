# GST Reconciliation Tool

A comprehensive tool for Indian Chartered Accountants to reconcile GSTR-2B data with Tally purchase records and identify Input Tax Credit (ITC) opportunities.

## Features

### Core Functionality
- **Smart File Processing**: Accepts GSTR-2B (JSON/Excel) and Tally export files (Excel/CSV)
- **Intelligent Matching**: Uses fuzzy matching algorithms to match invoices even with minor discrepancies
- **ITC Opportunity Detection**: Identifies invoices present in GSTR-2B but missing in books
- **Deadline Tracking**: Monitors 180-day ITC claim deadline with warnings at 150 days
- **Comprehensive Reporting**: Generates detailed Excel reports with multiple analysis sheets

### Technical Features
- RESTful API built with FastAPI
- API key authentication for security
- Large file support (up to 50MB)
- Automatic result caching (24 hours)
- CORS enabled for web integration
- OpenAPI documentation

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup Steps

1. Clone the repository:
```bash
git clone <repository-url>
cd gst-reconciliation-tool
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env file with your settings
```

5. Run the application:
```bash
python main.py
```

The application will start on http://localhost:8000

## Configuration

Edit the `.env` file to configure:

```env
# API Configuration
API_KEY=your-secure-api-key-here
SECRET_KEY=your-jwt-secret-key-here
ENVIRONMENT=production

# Matching Configuration
FUZZY_MATCH_THRESHOLD=80  # Minimum similarity score (0-100)
AMOUNT_TOLERANCE=10  # Amount tolerance in INR
ITC_DEADLINE_DAYS=180
WARNING_THRESHOLD_DAYS=150

# File Upload Configuration
MAX_FILE_SIZE_MB=50
```

## Usage

### Web Interface
1. Open http://localhost:8000/static/index.html
2. Enter your API key
3. Upload GSTR-2B and Tally files
4. Click "Start Reconciliation"
5. Download the Excel report

### API Usage

#### 1. Reconcile Files
```bash
curl -X POST "http://localhost:8000/api/reconcile" \\
  -H "Authorization: Bearer your-api-key" \\
  -F "gstr2b_file=@path/to/gstr2b.json" \\
  -F "tally_file=@path/to/tally.xlsx"
```

Response:
```json
{
  "report_id": "uuid-string",
  "summary": {
    "matched_count": 45,
    "available_itc_amount": 125000.50,
    "deadline_warnings": 3
  },
  "download_url": "/api/download/uuid-string"
}
```

#### 2. Download Report
```bash
curl -X GET "http://localhost:8000/api/download/{report_id}" \\
  -H "Authorization: Bearer your-api-key" \\
  -o reconciliation_report.xlsx
```

## File Formats

### GSTR-2B JSON Format
```json
{
  "gstin": "29ABCDE1234F1Z5",
  "fp": "042024",
  "b2b": [{
    "ctin": "29XYZAB5678C1D2",
    "inv": [{
      "inum": "INV-001",
      "idt": "15-04-2024",
      "val": 118000,
      "itms": [{
        "txval": 100000,
        "igst": 18000
      }]
    }]
  }]
}
```

### Tally Export Format
Required columns (flexible naming supported):
- Date / Invoice Date / Voucher Date
- Party Name / Particulars / Ledger Name
- GSTIN/UIN / GSTIN / GST No.
- Invoice No. / Vch No. / Invoice Number
- Taxable Value / Taxable Amount
- IGST @ 18% / Integrated Tax Amount
- CGST / Central Tax Amount
- SGST / State Tax Amount
- Total / Invoice Value
- ITC Claimed (optional - for filtering already claimed ITC)

## Matching Logic

The tool uses a sophisticated matching algorithm:

1. **Exact Matching**: First attempts to match by GSTIN + normalized invoice number
2. **Fuzzy Matching**: Uses Levenshtein distance for invoice numbers with same GSTIN
3. **Amount Tolerance**: Allows ±₹10 difference in invoice amounts
4. **Date Flexibility**: Flags date mismatches but doesn't reject matches

## Report Structure

The generated Excel report contains:

1. **Summary Sheet**: Overview with total ITC opportunities
2. **ITC Opportunities**: Invoices missing in books (claimable ITC)
3. **Matched Invoices**: Successfully reconciled invoices
4. **Pending Vendor Filing**: Invoices in books but not in GSTR-2B
5. **Mismatches**: Invoices with amount/date discrepancies
6. **Instructions**: Guide for using the report

## Testing

Use the sample files in `test_data/` directory:
- `sample_gstr2b.json`: Sample GSTR-2B data
- `sample_tally_data.csv`: Sample Tally export

## API Documentation

Interactive API documentation available at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Troubleshooting

### Common Issues

1. **File Upload Errors**
   - Ensure file size is under 50MB
   - Check file format (JSON/Excel for GSTR-2B, Excel/CSV for Tally)

2. **Matching Issues**
   - Verify GSTIN format in both files
   - Check invoice number formats
   - Adjust FUZZY_MATCH_THRESHOLD if needed

3. **API Key Errors**
   - Ensure API key matches the one in .env file
   - Include "Bearer " prefix in Authorization header

## Security Considerations

1. Change default API key and secret key in production
2. Use HTTPS in production environments
3. Implement rate limiting for API endpoints
4. Regular cleanup of uploaded files and cached data
5. Consider using Redis for caching in production

## Performance Tips

1. For large files, increase available memory
2. Use SSD storage for better file I/O
3. Implement background job processing for very large reconciliations
4. Consider database storage for persistent results

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues or questions:
1. Check the FAQ section in documentation
2. Review existing GitHub issues
3. Create a new issue with detailed information

## Roadmap

- [ ] Add support for GSTR-2A format
- [ ] Implement bulk reconciliation
- [ ] Add email notifications
- [ ] Create desktop application
- [ ] Add multi-user support
- [ ] Implement audit trail
- [ ] Add more export formats (PDF, CSV)