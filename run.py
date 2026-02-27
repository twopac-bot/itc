#!/usr/bin/env python3
"""
GST Reconciliation Tool Startup Script
"""

import uvicorn
from main import app

if __name__ == "__main__":
    print("🚀 Starting GST Reconciliation Tool...")
    print("📊 Reconcile GSTR-2B with Tally Purchase Data")
    print("🌐 Web UI: http://localhost:8000/static/index.html")
    print("📚 API Docs: http://localhost:8000/docs")
    print("🔑 Default API Key: default-api-key-change-this")
    print("\n" + "="*50)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )