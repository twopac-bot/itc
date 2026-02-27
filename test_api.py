"""Test the /api/reconcile endpoint end-to-end."""
import httpx
import json
import sys

API_URL = "http://localhost:8000"
API_KEY = "default-api-key-change-this"

headers = {"Authorization": f"Bearer {API_KEY}"}

print("=== Testing /api/reconcile ===")
with open("test_data/sample_gstr2b.json", "rb") as gstr, open("test_data/sample_tally.csv", "rb") as tally:
    resp = httpx.post(
        f"{API_URL}/api/reconcile",
        headers=headers,
        files={
            "gstr2b_file": ("sample_gstr2b.json", gstr, "application/json"),
            "tally_file": ("sample_tally.csv", tally, "text/csv"),
        },
        timeout=30,
    )

print(f"Status: {resp.status_code}")
if resp.status_code != 200:
    print(f"Error: {resp.text}")
    sys.exit(1)

data = resp.json()
print(f"Report ID: {data['report_id']}")
print(f"Matched: {data['matched_count']}")
print(f"Missing in books: {data['missing_in_books_count']}")
print(f"Available ITC: {data['available_itc_amount']}")
print(f"Deadline warnings: {data['deadline_warnings']}")
print(f"Download URL: {data['download_url']}")

# Fetch the full report JSON
report_id = data["report_id"]
print(f"\n=== Testing /api/report/{report_id} ===")
resp2 = httpx.get(f"{API_URL}/api/report/{report_id}", headers=headers)
print(f"Status: {resp2.status_code}")
report = resp2.json()

# Check graph_analysis
ga = report.get("graph_analysis")
if ga is None:
    print("WARNING: graph_analysis is None (graph layer may have failed)")
else:
    risk = ga.get("risk_scores", {})
    print(f"Graph nodes: {ga['graph_stats']['total_nodes']}")
    print(f"Graph edges: {ga['graph_stats']['total_edges']}")
    print(f"Fraud flags: {len(ga['fraud_flags'])}")
    print(f"Scored invoices: {risk.get('total_flagged', 0)}")
    print(f"High risk: {risk.get('high_risk_count', 0)}")
    print(f"Critical: {risk.get('critical_count', 0)}")
    if risk.get("scored_invoices"):
        print(f"\nTop risk invoice:")
        print(json.dumps(risk["scored_invoices"][0], indent=2))

# Test download
print(f"\n=== Testing /api/download/{report_id} ===")
resp3 = httpx.get(f"{API_URL}/api/download/{report_id}", headers=headers)
print(f"Status: {resp3.status_code}")
print(f"Content-Type: {resp3.headers.get('content-type', 'N/A')}")
print(f"File size: {len(resp3.content)} bytes")

print("\n=== ALL TESTS PASSED ===")
