"""Smoke test for graph engine integration."""
import json
from reconciliation.parser import GSTRParser
from reconciliation.matcher import InvoiceMatcher
from graph_engine import build_graph, run_fraud_analysis, compute_risk_scores

# Parse GSTR-2B
gstr2b_data = GSTRParser.parse_gstr2b("test_data/sample_gstr2b.json")
inv_count = len(gstr2b_data["invoices"])
print(f"Parsed {inv_count} GSTR-2B invoices")

# Simulate empty tally (all invoices will be missing_in_books)
tally_data = []
matcher = InvoiceMatcher()
result = matcher.reconcile(gstr2b_data, tally_data)

report_data = {
    "summary": result.summary,
    "matched_invoices": result.matched_invoices,
    "missing_in_books": result.missing_in_books,
    "pending_vendor_filing": result.pending_vendor_filing,
    "mismatches": result.mismatches,
}

# Build graph
graph = build_graph(gstr2b_data, report_data)
print(f"Graph: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges")

# Fraud analysis
fraud_result = run_fraud_analysis(graph)
print(f"Fraud flags: {len(fraud_result.flags)}")
for f in fraud_result.flags[:3]:
    print(f"  [{f.severity}] {f.rule_name}: {f.description[:90]}...")

# Risk scoring
risk_result = compute_risk_scores(
    graph, fraud_result, buyer_gstin=gstr2b_data["gstin"]
)
print(f"Risk scores: {len(risk_result.scored_invoices)} invoices")
for s in risk_result.scored_invoices[:3]:
    print(f"  {s.invoice_number} -> score={s.risk_score} ({s.risk_category}) rules={s.triggered_rules}")

print()
print("=== SAMPLE FRAUD OUTPUT (first scored invoice) ===")
if risk_result.scored_invoices:
    print(json.dumps(risk_result.scored_invoices[0].to_dict(), indent=2))
