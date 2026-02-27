"""
Graph Builder — converts reconciliation output into a NetworkX DiGraph.

Node types
----------
- TAXPAYER   : keyed by GSTIN (both buyer and each supplier)
- INVOICE    : keyed by a deterministic composite ID
- RETURN     : represents the GSTR-2B filing period

Edge types
----------
- ISSUED           : Supplier  → Invoice
- RECEIVED         : Buyer     → Invoice
- CLAIMED_IN       : Invoice   → Return  (only for matched / available ITC)
- FILED_IN         : Return    → Taxpayer
- MATCHED          : GSTR-2B inv  ↔ Tally inv (bi-directional convenience)
- MISMATCHED       : GSTR-2B inv  ↔ Tally inv
- PAID_TAX         : Taxpayer  → TaxPayment  (stub — for future data)

The builder never touches matcher / parser logic.  It receives the
already-processed ``report_data`` dict produced by ``InvoiceMatcher.reconcile``
and wraps it into a graph structure.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional

import networkx as nx  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants — node / edge type labels (kept as strings so serialisation to
# Neo4j / JSON is trivial later).
# ---------------------------------------------------------------------------

NODE_TAXPAYER = "TAXPAYER"
NODE_INVOICE = "INVOICE"
NODE_RETURN = "RETURN"
NODE_TAX_PAYMENT = "TAX_PAYMENT"

EDGE_ISSUED = "ISSUED"
EDGE_RECEIVED = "RECEIVED"
EDGE_CLAIMED_IN = "CLAIMED_IN"
EDGE_FILED_IN = "FILED_IN"
EDGE_MATCHED = "MATCHED"
EDGE_MISMATCHED = "MISMATCHED"
EDGE_PAID_TAX = "PAID_TAX"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _invoice_node_id(supplier_gstin: str, invoice_number: str, source: str) -> str:
    """Deterministic, collision-resistant node ID for an invoice.

    Parameters
    ----------
    supplier_gstin : str
        GSTIN of the supplier that issued the invoice.
    invoice_number : str
        Raw invoice number string.
    source : str
        ``"gstr2b"`` or ``"tally"`` — keeps IDs unique across data sources so
        we can create MATCHED / MISMATCHED edges between them.

    Returns
    -------
    str
        A short hex-digest prefixed with ``INV:``.
    """
    raw = f"{supplier_gstin}|{invoice_number}|{source}".upper()
    digest = hashlib.sha256(raw.encode()).hexdigest()[:12]
    return f"INV:{source.upper()}:{digest}"


def _taxpayer_node_id(gstin: str) -> str:
    """Node ID for a taxpayer (GSTIN-based)."""
    return f"TP:{gstin.upper()}"


def _return_node_id(gstin: str, filing_period: str) -> str:
    """Node ID for a GSTR-2B return filing."""
    return f"RET:GSTR2B:{gstin.upper()}:{filing_period}"


# ---------------------------------------------------------------------------
# GraphBuilder
# ---------------------------------------------------------------------------

class GraphBuilder:
    """Constructs a ``networkx.DiGraph`` from reconciliation output.

    Usage
    -----
    >>> builder = GraphBuilder()
    >>> G = builder.build(gstr2b_data, report_data)

    The resulting graph can be passed directly to ``FraudDetector`` and
    ``RiskScorer``.
    """

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(
        self,
        gstr2b_data: Dict[str, Any],
        report_data: Dict[str, Any],
    ) -> nx.DiGraph:
        """Build the full knowledge graph.

        Parameters
        ----------
        gstr2b_data : dict
            Raw parsed GSTR-2B output from ``GSTRParser.parse_gstr2b``.
            Must contain keys ``gstin``, ``filing_period``, ``invoices``.
        report_data : dict
            The reconciliation result dict with keys:
            ``matched_invoices``, ``missing_in_books``,
            ``pending_vendor_filing``, ``mismatches``, ``summary``.

        Returns
        -------
        nx.DiGraph
        """
        self._graph = nx.DiGraph()

        buyer_gstin: str = gstr2b_data.get("gstin", "UNKNOWN")
        filing_period: str = gstr2b_data.get("filing_period", "UNKNOWN")

        # 1. Buyer taxpayer node
        self._add_taxpayer(buyer_gstin, role="buyer")

        # 2. Return node for this GSTR-2B
        return_nid = self._add_return(buyer_gstin, filing_period)
        self._graph.add_edge(
            return_nid,
            _taxpayer_node_id(buyer_gstin),
            relation=EDGE_FILED_IN,
        )

        # 3. Matched invoices — both sources present
        for entry in report_data.get("matched_invoices", []):
            self._add_matched_pair(entry, buyer_gstin, return_nid)

        # 4. Missing in books — only in GSTR-2B
        for entry in report_data.get("missing_in_books", []):
            inv = entry["invoice"]
            gstr_nid = self._add_invoice_from_gstr2b(inv, buyer_gstin, return_nid)
            # Mark with metadata for fraud detection
            self._graph.nodes[gstr_nid]["missing_in_books"] = True
            self._graph.nodes[gstr_nid]["itc_amount"] = entry.get("itc_amount", 0)
            self._graph.nodes[gstr_nid]["days_old"] = entry.get("days_old", 0)
            self._graph.nodes[gstr_nid]["expired"] = entry.get("expired", False)

        # 5. Pending vendor filing — only in Tally
        for entry in report_data.get("pending_vendor_filing", []):
            inv = entry["invoice"]
            self._add_invoice_from_tally(inv, buyer_gstin)
            # No GSTR-2B counterpart → cannot link to return

        # 6. Mismatched invoices
        for entry in report_data.get("mismatches", []):
            self._add_mismatched_pair(entry, buyer_gstin, return_nid)

        logger.info(
            "Graph built: %d nodes, %d edges",
            self._graph.number_of_nodes(),
            self._graph.number_of_edges(),
        )
        return self._graph

    # ------------------------------------------------------------------
    # Internal helpers — node creation
    # ------------------------------------------------------------------

    def _add_taxpayer(self, gstin: str, role: str = "supplier") -> str:
        nid = _taxpayer_node_id(gstin)
        if nid not in self._graph:
            self._graph.add_node(
                nid,
                node_type=NODE_TAXPAYER,
                gstin=gstin,
                role=role,
            )
        return nid

    def _add_return(self, gstin: str, filing_period: str) -> str:
        nid = _return_node_id(gstin, filing_period)
        if nid not in self._graph:
            self._graph.add_node(
                nid,
                node_type=NODE_RETURN,
                gstin=gstin,
                filing_period=filing_period,
                return_type="GSTR2B",
            )
        return nid

    def _add_invoice_node(
        self,
        supplier_gstin: str,
        invoice_number: str,
        source: str,
        **attrs: Any,
    ) -> str:
        nid = _invoice_node_id(supplier_gstin, invoice_number, source)
        if nid not in self._graph:
            self._graph.add_node(
                nid,
                node_type=NODE_INVOICE,
                supplier_gstin=supplier_gstin,
                invoice_number=invoice_number,
                source=source,
                **attrs,
            )
        return nid

    # ------------------------------------------------------------------
    # Internal helpers — wiring edges for different reconciliation buckets
    # ------------------------------------------------------------------

    def _add_invoice_from_gstr2b(
        self,
        inv: Dict[str, Any],
        buyer_gstin: str,
        return_nid: str,
    ) -> str:
        """Add GSTR-2B invoice node plus supplier/buyer/return edges."""
        supplier_gstin = inv["supplier_gstin"]
        inv_num = inv["invoice_number"]

        supplier_nid = self._add_taxpayer(supplier_gstin, role="supplier")
        inv_nid = self._add_invoice_node(
            supplier_gstin,
            inv_num,
            source="gstr2b",
            invoice_date=inv.get("invoice_date", ""),
            taxable_value=inv.get("taxable_value", 0),
            total_value=inv.get("total_value", 0),
            total_tax=inv.get("total_tax", 0),
            igst=inv.get("igst", 0),
            cgst=inv.get("cgst", 0),
            sgst=inv.get("sgst", 0),
        )
        buyer_nid = _taxpayer_node_id(buyer_gstin)

        # Supplier → Invoice (ISSUED)
        self._graph.add_edge(supplier_nid, inv_nid, relation=EDGE_ISSUED)
        # Buyer ← Invoice (RECEIVED)
        self._graph.add_edge(inv_nid, buyer_nid, relation=EDGE_RECEIVED)
        # Invoice → Return (CLAIMED_IN)
        self._graph.add_edge(inv_nid, return_nid, relation=EDGE_CLAIMED_IN)

        return inv_nid

    def _add_invoice_from_tally(
        self,
        inv: Dict[str, Any],
        buyer_gstin: str,
    ) -> str:
        """Add Tally-side invoice node (no return linkage)."""
        supplier_gstin = inv.get("gstin", "UNKNOWN")
        inv_num = inv.get("invoice_number", "UNKNOWN")

        if supplier_gstin and supplier_gstin != "UNKNOWN":
            self._add_taxpayer(supplier_gstin, role="supplier")

        inv_nid = self._add_invoice_node(
            supplier_gstin,
            inv_num,
            source="tally",
            invoice_date=inv.get("date", ""),
            party_name=inv.get("party_name", ""),
            taxable_value=inv.get("taxable_value", 0),
            total_value=inv.get("total_value", 0),
            total_tax=inv.get("total_tax", 0),
            igst=inv.get("igst", 0),
            cgst=inv.get("cgst", 0),
            sgst=inv.get("sgst", 0),
            itc_claimed=inv.get("itc_claimed", False),
        )

        buyer_nid = _taxpayer_node_id(buyer_gstin)
        if supplier_gstin and supplier_gstin != "UNKNOWN":
            self._graph.add_edge(
                _taxpayer_node_id(supplier_gstin),
                inv_nid,
                relation=EDGE_ISSUED,
            )
        self._graph.add_edge(inv_nid, buyer_nid, relation=EDGE_RECEIVED)

        return inv_nid

    def _add_matched_pair(
        self,
        entry: Dict[str, Any],
        buyer_gstin: str,
        return_nid: str,
    ) -> None:
        """Wire a matched pair (GSTR-2B ↔ Tally) with a MATCHED edge."""
        gstr_inv = entry["gstr2b"]
        tally_inv = entry["tally"]

        gstr_nid = self._add_invoice_from_gstr2b(gstr_inv, buyer_gstin, return_nid)
        tally_nid = self._add_invoice_from_tally(tally_inv, buyer_gstin)

        self._graph.add_edge(
            gstr_nid,
            tally_nid,
            relation=EDGE_MATCHED,
            match_type=entry.get("match_type", "exact"),
            match_score=entry.get("match_score", 100),
            itc_amount=entry.get("itc_amount", 0),
        )

    def _add_mismatched_pair(
        self,
        entry: Dict[str, Any],
        buyer_gstin: str,
        return_nid: str,
    ) -> None:
        """Wire a mismatched pair with a MISMATCHED edge."""
        gstr_inv = entry["gstr2b"]
        tally_inv = entry["tally"]

        gstr_nid = self._add_invoice_from_gstr2b(gstr_inv, buyer_gstin, return_nid)
        tally_nid = self._add_invoice_from_tally(tally_inv, buyer_gstin)

        self._graph.add_edge(
            gstr_nid,
            tally_nid,
            relation=EDGE_MISMATCHED,
            match_score=entry.get("match_score", 0),
            mismatch_details=entry.get("mismatches", []),
            itc_amount=entry.get("itc_amount", 0),
        )

    # ------------------------------------------------------------------
    # Introspection helpers (useful for tests / debugging)
    # ------------------------------------------------------------------

    def get_nodes_by_type(self, node_type: str) -> List[str]:
        """Return node IDs filtered by ``node_type`` attribute."""
        return [
            n
            for n, d in self._graph.nodes(data=True)
            if d.get("node_type") == node_type
        ]

    def get_edges_by_relation(self, relation: str) -> List[tuple]:
        """Return ``(u, v)`` tuples filtered by ``relation`` attribute."""
        return [
            (u, v)
            for u, v, d in self._graph.edges(data=True)
            if d.get("relation") == relation
        ]


# ---------------------------------------------------------------------------
# Module-level convenience function
# ---------------------------------------------------------------------------

def build_graph(
    gstr2b_data: Dict[str, Any],
    report_data: Dict[str, Any],
) -> nx.DiGraph:
    """One-shot convenience wrapper around :class:`GraphBuilder`.

    Parameters
    ----------
    gstr2b_data : dict
        Parsed GSTR-2B data (from ``GSTRParser``).
    report_data : dict
        Reconciliation result dict (from ``InvoiceMatcher``).

    Returns
    -------
    nx.DiGraph
        The fully constructed knowledge graph.
    """
    builder = GraphBuilder()
    return builder.build(gstr2b_data, report_data)
