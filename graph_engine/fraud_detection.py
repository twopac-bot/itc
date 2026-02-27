"""
Fraud Detection — modular rule functions operating on the knowledge graph.

Each public function accepts a ``networkx.DiGraph`` produced by
:func:`graph_engine.graph_builder.build_graph` and returns a list of
**flagged invoice node IDs** together with human-readable explanations.

The module is intentionally stateless: no database, no side-effects.
Results are pure data that the risk-scoring layer can aggregate.

Rule catalogue
--------------
1. ``detect_missing_chain``         — ITC claimed but invoice not in books
2. ``detect_high_degree_suppliers`` — suppliers linked to abnormally many invoices
3. ``detect_cycles``                — circular invoice patterns (carousel fraud)
4. ``detect_itc_without_tax_payment`` — ITC claimed but supplier has no tax-payment record
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx  # type: ignore[import-untyped]

from graph_engine.graph_builder import (
    EDGE_CLAIMED_IN,
    EDGE_ISSUED,
    EDGE_MATCHED,
    EDGE_MISMATCHED,
    EDGE_PAID_TAX,
    EDGE_RECEIVED,
    NODE_INVOICE,
    NODE_TAX_PAYMENT,
    NODE_TAXPAYER,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class FraudFlag:
    """A single fraud signal attached to an invoice node."""

    invoice_node_id: str
    rule_name: str
    description: str
    severity: str = "medium"  # low | medium | high | critical
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FraudAnalysisResult:
    """Aggregated output of all fraud-detection rules."""

    flags: List[FraudFlag] = field(default_factory=list)
    flagged_invoices: Set[str] = field(default_factory=set)
    rule_summary: Dict[str, int] = field(default_factory=dict)

    def add(self, flag: FraudFlag) -> None:
        self.flags.append(flag)
        self.flagged_invoices.add(flag.invoice_node_id)
        self.rule_summary[flag.rule_name] = self.rule_summary.get(flag.rule_name, 0) + 1


# ---------------------------------------------------------------------------
# Rule 1 — Missing Chain
# ---------------------------------------------------------------------------

def detect_missing_chain(graph: nx.DiGraph) -> List[FraudFlag]:
    """Flag invoices present in GSTR-2B but **not recorded** in the buyer's books.

    These show up in the graph as GSTR-2B invoice nodes that have a
    ``CLAIMED_IN`` edge to the return but no corresponding ``MATCHED``
    edge to a Tally-side invoice node.

    This is the most common ITC-fraud vector: a fake supplier files
    GSTR-1 with fabricated invoices, the buyer claims ITC via GSTR-2B,
    but no actual purchase was booked.
    """
    flags: List[FraudFlag] = []

    for node, data in graph.nodes(data=True):
        if data.get("node_type") != NODE_INVOICE:
            continue
        if data.get("source") != "gstr2b":
            continue

        # Check if this GSTR-2B invoice has a MATCHED edge
        has_match = any(
            d.get("relation") == EDGE_MATCHED
            for _, _, d in graph.out_edges(node, data=True)
        )
        if has_match:
            continue

        # Also check if it has a MISMATCHED edge (partial match — different signal)
        has_mismatch = any(
            d.get("relation") == EDGE_MISMATCHED
            for _, _, d in graph.out_edges(node, data=True)
        )

        if has_mismatch:
            # Mismatch is a softer signal — still flag but lower severity
            flags.append(FraudFlag(
                invoice_node_id=node,
                rule_name="missing_chain",
                description=(
                    f"Invoice {data.get('invoice_number')} from supplier "
                    f"{data.get('supplier_gstin')} has value mismatches — "
                    "potential fictitious or inflated invoice."
                ),
                severity="medium",
                metadata={
                    "supplier_gstin": data.get("supplier_gstin", ""),
                    "invoice_number": data.get("invoice_number", ""),
                    "total_tax": data.get("total_tax", 0),
                    "sub_reason": "mismatch_present",
                },
            ))
        else:
            # No counterpart at all — strongest signal
            flags.append(FraudFlag(
                invoice_node_id=node,
                rule_name="missing_chain",
                description=(
                    f"Invoice {data.get('invoice_number')} from supplier "
                    f"{data.get('supplier_gstin')} appears in GSTR-2B but has "
                    "no corresponding entry in books — possible phantom invoice."
                ),
                severity="high",
                metadata={
                    "supplier_gstin": data.get("supplier_gstin", ""),
                    "invoice_number": data.get("invoice_number", ""),
                    "total_tax": data.get("total_tax", 0),
                    "sub_reason": "no_books_entry",
                },
            ))

    logger.info("detect_missing_chain: %d flags", len(flags))
    return flags


# ---------------------------------------------------------------------------
# Rule 2 — High-Degree Suppliers
# ---------------------------------------------------------------------------

def detect_high_degree_suppliers(
    graph: nx.DiGraph,
    *,
    z_score_threshold: float = 2.0,
    absolute_minimum: int = 5,
) -> List[FraudFlag]:
    """Flag invoices from suppliers whose out-degree (ISSUED edges) is
    statistically anomalous compared to peer suppliers.

    A supplier issuing far more invoices than others in the same filing
    period may be a bill-trading entity.

    Parameters
    ----------
    z_score_threshold : float
        Number of standard deviations above mean to consider anomalous.
    absolute_minimum : int
        Minimum invoice count to even consider flagging (avoids noise on
        very small datasets).
    """
    flags: List[FraudFlag] = []

    # Collect per-supplier invoice counts
    supplier_invoice_counts: Dict[str, List[str]] = {}
    for node, data in graph.nodes(data=True):
        if data.get("node_type") != NODE_TAXPAYER:
            continue
        if data.get("role") != "supplier":
            continue

        issued_invoices = [
            v
            for _, v, d in graph.out_edges(node, data=True)
            if d.get("relation") == EDGE_ISSUED
        ]
        if issued_invoices:
            supplier_invoice_counts[node] = issued_invoices

    if not supplier_invoice_counts:
        return flags

    counts = [len(v) for v in supplier_invoice_counts.values()]

    # Need at least 2 suppliers to compute meaningful statistics
    if len(counts) < 2:
        return flags

    mean_count = statistics.mean(counts)
    stdev_count = statistics.stdev(counts)

    # Avoid division-by-zero when stdev is 0 (all suppliers have same count)
    if stdev_count == 0:
        return flags

    for supplier_nid, invoice_nids in supplier_invoice_counts.items():
        count = len(invoice_nids)
        if count < absolute_minimum:
            continue

        z = (count - mean_count) / stdev_count
        if z < z_score_threshold:
            continue

        supplier_gstin = graph.nodes[supplier_nid].get("gstin", "")
        for inv_nid in invoice_nids:
            inv_data = graph.nodes.get(inv_nid, {})
            flags.append(FraudFlag(
                invoice_node_id=inv_nid,
                rule_name="high_degree_supplier",
                description=(
                    f"Supplier {supplier_gstin} has unusually high invoice volume "
                    f"({count} invoices, z-score={z:.2f}) — potential bill-trading entity."
                ),
                severity="medium",
                metadata={
                    "supplier_gstin": supplier_gstin,
                    "invoice_number": inv_data.get("invoice_number", ""),
                    "supplier_invoice_count": count,
                    "z_score": round(z, 2),
                    "mean_invoice_count": round(mean_count, 2),
                },
            ))

    logger.info("detect_high_degree_suppliers: %d flags", len(flags))
    return flags


# ---------------------------------------------------------------------------
# Rule 3 — Cycle Detection (Carousel Fraud)
# ---------------------------------------------------------------------------

def detect_cycles(graph: nx.DiGraph) -> List[FraudFlag]:
    """Flag invoices participating in circular supplier-buyer-supplier chains.

    In carousel fraud, the same goods (or fictitious goods) are invoiced
    in a loop: A → B → C → A.  Each entity claims ITC on the "purchase"
    leg, generating fraudulent refunds.

    We build a **taxpayer-only projection** (collapse invoices) and run
    cycle detection on it, then map flagged taxpayer edges back to the
    original invoice nodes.
    """
    flags: List[FraudFlag] = []

    # Build taxpayer-projection: directed edge from supplier → buyer
    # whenever there is a chain  Supplier -ISSUED-> Invoice -RECEIVED-> Buyer
    tp_graph: nx.DiGraph = nx.DiGraph()

    for node, data in graph.nodes(data=True):
        if data.get("node_type") != NODE_INVOICE:
            continue

        issuers = [
            u
            for u, _, d in graph.in_edges(node, data=True)
            if d.get("relation") == EDGE_ISSUED
        ]
        receivers = [
            v
            for _, v, d in graph.out_edges(node, data=True)
            if d.get("relation") == EDGE_RECEIVED
        ]

        for issuer in issuers:
            for receiver in receivers:
                if issuer != receiver:
                    # Store originating invoice IDs on the edge
                    if tp_graph.has_edge(issuer, receiver):
                        tp_graph[issuer][receiver]["invoices"].append(node)
                    else:
                        tp_graph.add_edge(issuer, receiver, invoices=[node])

    # Find simple cycles in the taxpayer projection
    try:
        cycles = list(nx.simple_cycles(tp_graph))
    except nx.NetworkXError:
        cycles = []

    # Map cycles back to invoice nodes
    flagged_invoices: Set[str] = set()
    for cycle in cycles:
        cycle_gstins = [
            graph.nodes[n].get("gstin", n) for n in cycle if n in graph.nodes
        ]
        # Collect all invoice nodes on the cycle edges
        for i in range(len(cycle)):
            u = cycle[i]
            v = cycle[(i + 1) % len(cycle)]
            if tp_graph.has_edge(u, v):
                for inv_nid in tp_graph[u][v].get("invoices", []):
                    if inv_nid in flagged_invoices:
                        continue
                    flagged_invoices.add(inv_nid)
                    inv_data = graph.nodes.get(inv_nid, {})
                    flags.append(FraudFlag(
                        invoice_node_id=inv_nid,
                        rule_name="cycle_participation",
                        description=(
                            f"Invoice {inv_data.get('invoice_number', '?')} participates in "
                            f"a circular chain: {' → '.join(cycle_gstins)} — "
                            "potential carousel fraud."
                        ),
                        severity="critical",
                        metadata={
                            "supplier_gstin": inv_data.get("supplier_gstin", ""),
                            "invoice_number": inv_data.get("invoice_number", ""),
                            "cycle_gstins": cycle_gstins,
                            "cycle_length": len(cycle),
                        },
                    ))

    logger.info("detect_cycles: %d flags", len(flags))
    return flags


# ---------------------------------------------------------------------------
# Rule 4 — ITC Without Tax Payment
# ---------------------------------------------------------------------------

def detect_itc_without_tax_payment(graph: nx.DiGraph) -> List[FraudFlag]:
    """Flag invoices where the supplier has **no recorded tax payment**.

    This rule checks whether the supplier taxpayer node has at least one
    outgoing ``PAID_TAX`` edge.  If the current dataset does not include
    tax-payment data the supplier will be flagged — callers can filter
    these results when payment data is unavailable.

    When integrated with GSTR-3B / Challan data in the future, this
    becomes a strong signal for *tax-evasion-linked ITC fraud*.
    """
    flags: List[FraudFlag] = []

    # Build set of suppliers that have PAID_TAX edges
    suppliers_with_payment: Set[str] = set()
    for u, _, d in graph.edges(data=True):
        if d.get("relation") == EDGE_PAID_TAX:
            suppliers_with_payment.add(u)

    # Walk GSTR-2B invoices → check their issuing supplier
    for node, data in graph.nodes(data=True):
        if data.get("node_type") != NODE_INVOICE:
            continue
        if data.get("source") != "gstr2b":
            continue

        supplier_gstin = data.get("supplier_gstin", "")
        supplier_nid = f"TP:{supplier_gstin.upper()}" if supplier_gstin else None

        if supplier_nid and supplier_nid not in suppliers_with_payment:
            flags.append(FraudFlag(
                invoice_node_id=node,
                rule_name="itc_without_tax_payment",
                description=(
                    f"Supplier {supplier_gstin} has no recorded tax payment — ITC on "
                    f"invoice {data.get('invoice_number', '?')} may be at risk."
                ),
                severity="low",
                metadata={
                    "supplier_gstin": supplier_gstin,
                    "invoice_number": data.get("invoice_number", ""),
                    "total_tax": data.get("total_tax", 0),
                    "note": (
                        "Tax-payment data not yet integrated. "
                        "Severity will increase when GSTR-3B data is available."
                    ),
                },
            ))

    logger.info("detect_itc_without_tax_payment: %d flags", len(flags))
    return flags


# ---------------------------------------------------------------------------
# FraudDetector — orchestrator
# ---------------------------------------------------------------------------

class FraudDetector:
    """Runs all fraud-detection rules and returns an aggregated result.

    The class is a thin orchestrator; all heavy logic lives in the
    individual rule functions above so they remain independently testable.
    """

    # Registry of rule functions — extend by appending here.
    _rules = [
        detect_missing_chain,
        detect_high_degree_suppliers,
        detect_cycles,
        detect_itc_without_tax_payment,
    ]

    def __init__(self, *, enable_tax_payment_rule: bool = False) -> None:
        """
        Parameters
        ----------
        enable_tax_payment_rule : bool
            Set to ``True`` once GSTR-3B / Challan data is integrated.
            When ``False``, ``detect_itc_without_tax_payment`` still runs
            but its flags carry ``severity="low"`` so the risk-scoring
            layer can optionally ignore them.
        """
        self.enable_tax_payment_rule = enable_tax_payment_rule

    def analyze(self, graph: nx.DiGraph) -> FraudAnalysisResult:
        """Execute all registered rules against *graph*.

        Returns
        -------
        FraudAnalysisResult
        """
        result = FraudAnalysisResult()

        for rule_fn in self._rules:
            rule_name = rule_fn.__name__

            # Optionally skip tax-payment rule when data is absent
            if rule_name == "detect_itc_without_tax_payment" and not self.enable_tax_payment_rule:
                logger.debug("Skipping %s (tax-payment data not enabled)", rule_name)
                continue

            try:
                flags = rule_fn(graph)
                for flag in flags:
                    result.add(flag)
            except Exception:
                logger.exception("Rule %s raised an exception — skipping", rule_name)

        logger.info(
            "Fraud analysis complete: %d total flags across %d invoices",
            len(result.flags),
            len(result.flagged_invoices),
        )
        return result


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

def run_fraud_analysis(
    graph: nx.DiGraph,
    *,
    enable_tax_payment_rule: bool = False,
) -> FraudAnalysisResult:
    """One-shot convenience wrapper around :class:`FraudDetector`.

    Parameters
    ----------
    graph : nx.DiGraph
        Knowledge graph from ``build_graph()``.
    enable_tax_payment_rule : bool
        Forward to ``FraudDetector.__init__``.

    Returns
    -------
    FraudAnalysisResult
    """
    detector = FraudDetector(enable_tax_payment_rule=enable_tax_payment_rule)
    return detector.analyze(graph)
