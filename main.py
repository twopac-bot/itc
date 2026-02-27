import os
import uuid
import json
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List
import logging

from fastapi import FastAPI, File, UploadFile, HTTPException, Depends, status, BackgroundTasks
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
import uvicorn
from jose import JWTError, jwt
from passlib.context import CryptContext

from reconciliation.parser import GSTRParser, TallyParser
from reconciliation.matcher import InvoiceMatcher
from reconciliation.report import ReportGenerator
from graph_engine import build_graph, run_fraud_analysis, compute_risk_scores


# Settings
class Settings(BaseSettings):
    api_key: str = "default-api-key-change-this"
    secret_key: str = "default-secret-key-change-this"
    environment: str = "development"
    max_file_size_mb: int = 50
    allowed_extensions: List[str] = Field(default=[".json", ".xlsx", ".xls", ".csv"])
    fuzzy_match_threshold: int = 80
    amount_tolerance: float = 10
    itc_deadline_days: int = 180
    warning_threshold_days: int = 150
    cache_ttl: int = 86400
    redis_url: str = "redis://localhost:6379"
    log_level: str = "INFO"
    log_file: str = "reconciliation.log"
    
    model_config = {"env_file": ".env"}


# Initialize
app = FastAPI(
    title="GST Reconciliation Tool",
    description="Reconcile GSTR-2B with Tally purchase data for Indian CAs",
    version="1.0.0"
)

settings = Settings()
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Setup logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('reconciliation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Storage
UPLOAD_DIR = Path("uploads")
REPORT_DIR = Path("reports")
UPLOAD_DIR.mkdir(exist_ok=True)
REPORT_DIR.mkdir(exist_ok=True)

# In-memory cache (replace with Redis in production)
reconciliation_cache: Dict[str, Dict] = {}


# Models
class ReconciliationRequest(BaseModel):
    gstr2b_file: str = Field(..., description="Path to GSTR-2B file")
    tally_file: str = Field(..., description="Path to Tally export file")


class ReconciliationResponse(BaseModel):
    report_id: str
    summary: Dict
    matched_count: int
    missing_in_books_count: int
    available_itc_amount: float
    deadline_warnings: int
    download_url: str


class ErrorResponse(BaseModel):
    detail: str
    error_type: str


# Authentication
def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API key"""
    token = credentials.credentials
    if token != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    return token


# Utility functions
def validate_file(file: UploadFile) -> None:
    """Validate uploaded file"""
    # Check file size
    file_size = 0
    for chunk in file.file:
        file_size += len(chunk)
    file.file.seek(0)
    
    if file_size > settings.max_file_size_mb * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail=f"File size exceeds {settings.max_file_size_mb}MB limit"
        )
    
    # Check file extension
    file_ext = Path(file.filename).suffix.lower()
    if file_ext not in settings.allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"File type {file_ext} not allowed. Allowed types: {settings.allowed_extensions}"
        )


def save_upload_file(upload_file: UploadFile) -> str:
    """Save uploaded file and return path"""
    try:
        file_id = str(uuid.uuid4())
        file_ext = Path(upload_file.filename).suffix
        file_path = UPLOAD_DIR / f"{file_id}{file_ext}"
        
        with open(file_path, "wb") as f:
            shutil.copyfileobj(upload_file.file, f)
        
        return str(file_path)
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}")
        raise HTTPException(status_code=500, detail="Error saving file")


def cleanup_old_files():
    """Clean up old uploaded files and reports"""
    try:
        cutoff_time = datetime.now() - timedelta(seconds=settings.cache_ttl)
        
        # Clean uploads
        for file_path in UPLOAD_DIR.glob("*"):
            if file_path.stat().st_mtime < cutoff_time.timestamp():
                file_path.unlink()
        
        # Clean reports
        for file_path in REPORT_DIR.glob("*.xlsx"):
            if file_path.stat().st_mtime < cutoff_time.timestamp():
                file_path.unlink()
        
        # Clean cache
        expired_keys = []
        for key, value in reconciliation_cache.items():
            if datetime.fromisoformat(value['timestamp']) < cutoff_time:
                expired_keys.append(key)
        
        for key in expired_keys:
            del reconciliation_cache[key]
            
    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")


# API Endpoints
@app.get("/", include_in_schema=False)
async def root():
    """Redirect to documentation"""
    return {"message": "GST Reconciliation Tool API. Visit /docs for API documentation."}


@app.post("/api/reconcile", response_model=ReconciliationResponse)
async def reconcile(
    background_tasks: BackgroundTasks,
    gstr2b_file: UploadFile = File(..., description="GSTR-2B file (JSON or Excel)"),
    tally_file: UploadFile = File(..., description="Tally export file (Excel or CSV)"),
    api_key: str = Depends(verify_api_key)
):
    """
    Perform GST reconciliation between GSTR-2B and Tally data.
    
    Returns reconciliation summary and report download URL.
    """
    # Validate files
    validate_file(gstr2b_file)
    validate_file(tally_file)
    
    # Save files
    gstr2b_path = save_upload_file(gstr2b_file)
    tally_path = save_upload_file(tally_file)
    
    try:
        # Parse files
        logger.info(f"Parsing GSTR-2B file: {gstr2b_file.filename}")
        gstr2b_data = GSTRParser.parse_gstr2b(gstr2b_path)
        
        logger.info(f"Parsing Tally file: {tally_file.filename}")
        tally_data = TallyParser.parse_tally_data(tally_path)
        
        # Perform reconciliation
        logger.info("Starting reconciliation process")
        matcher = InvoiceMatcher(
            fuzzy_threshold=settings.fuzzy_match_threshold,
            amount_tolerance=settings.amount_tolerance
        )
        result = matcher.reconcile(gstr2b_data, tally_data)
        
        # Generate report
        report_id = str(uuid.uuid4())
        report_path = REPORT_DIR / f"reconciliation_{report_id}.xlsx"
        
        logger.info(f"Generating Excel report: {report_id}")
        report_data = {
            'summary': result.summary,
            'matched_invoices': result.matched_invoices,
            'missing_in_books': result.missing_in_books,
            'pending_vendor_filing': result.pending_vendor_filing,
            'mismatches': result.mismatches
        }

        # --- Graph Intelligence Layer ---
        try:
            logger.info("Building knowledge graph")
            graph = build_graph(gstr2b_data, report_data)

            logger.info("Running fraud analysis")
            fraud_result = run_fraud_analysis(graph)

            logger.info("Computing risk scores")
            risk_result = compute_risk_scores(
                graph,
                fraud_result,
                buyer_gstin=gstr2b_data.get('gstin', ''),
            )

            report_data['graph_analysis'] = {
                'fraud_flags': [
                    {
                        'invoice_node_id': f.invoice_node_id,
                        'rule_name': f.rule_name,
                        'description': f.description,
                        'severity': f.severity,
                        'metadata': f.metadata,
                    }
                    for f in fraud_result.flags
                ],
                'risk_scores': risk_result.to_dict(),
                'graph_stats': {
                    'total_nodes': graph.number_of_nodes(),
                    'total_edges': graph.number_of_edges(),
                },
            }
            logger.info(
                "Graph analysis complete: %d fraud flags, %d scored invoices",
                len(fraud_result.flags),
                len(risk_result.scored_invoices),
            )
        except Exception as graph_err:
            # Graph layer is additive — never break existing reconciliation
            logger.warning("Graph analysis failed (non-fatal): %s", graph_err, exc_info=True)
            report_data['graph_analysis'] = None
        
        ReportGenerator.generate_excel_report(report_data, str(report_path))
        
        # Cache result
        reconciliation_cache[report_id] = {
            'data': report_data,
            'timestamp': datetime.now().isoformat(),
            'report_path': str(report_path)
        }
        
        # Schedule cleanup
        background_tasks.add_task(cleanup_old_files)
        
        # Log summary
        logger.info(f"Reconciliation completed - Report ID: {report_id}")
        logger.info(f"Summary: {result.summary}")
        
        return ReconciliationResponse(
            report_id=report_id,
            summary=result.summary,
            matched_count=result.summary['matched_count'],
            missing_in_books_count=result.summary['missing_in_books_count'],
            available_itc_amount=result.summary['available_itc_amount'],
            deadline_warnings=result.summary['deadline_warnings'],
            download_url=f"/api/download/{report_id}"
        )
        
    except Exception as e:
        logger.error(f"Reconciliation error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Reconciliation failed: {str(e)}"
        )
    finally:
        # Clean up uploaded files
        try:
            Path(gstr2b_path).unlink()
            Path(tally_path).unlink()
        except:
            pass


@app.get("/api/download/{report_id}")
async def download_report(
    report_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Download reconciliation report"""
    # Check cache
    if report_id not in reconciliation_cache:
        raise HTTPException(status_code=404, detail="Report not found")
    
    report_path = reconciliation_cache[report_id]['report_path']
    
    if not Path(report_path).exists():
        raise HTTPException(status_code=404, detail="Report file not found")
    
    return FileResponse(
        path=report_path,
        filename=f"GST_Reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.get("/api/report/{report_id}")
async def get_report_data(
    report_id: str,
    api_key: str = Depends(verify_api_key)
):
    """Get reconciliation report data as JSON"""
    if report_id not in reconciliation_cache:
        raise HTTPException(status_code=404, detail="Report not found")
    
    return reconciliation_cache[report_id]['data']


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(reconciliation_cache),
        "environment": settings.environment
    }


# Exception handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": exc.detail,
            "error_type": "http_error"
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error_type": "internal_error"
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.environment == "development",
        log_level=settings.log_level.lower()
    )