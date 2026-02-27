"""
Risk Scoring Engine — converts raw fraud flags into weighted risk scores.

Scoring weights (default)
-------------------------
- missing_chain            : 30
- high_degree_supplier     : 20
- cycle_participation      : 30
- itc_without_tax_payment  : 20
                             ----
  Maximum possible score   : 100

Output per invoice:
    {
        "invoice_id":      str,       # graph node ID
        "invoice_number":  str,
        "supplier_gstin":  str,
        "buyer_gstin":     str,
        "risk_score":      int,       # 0–100
        "risk_category":   str,       # LOW / MEDIUM / HIGH / CRITICAL
        "triggered_rules": [str],
        "details":         [dict],    # per-rule detail objects
    }

The scorer is deterministic and side-effect-free.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import networkx as nx  # type: ignore[import-untyped]

from graph_engine.fraud_detection import FraudAnalysisResult, FraudFlag
from graph_engine.graph_builder import NODE_INVOICE

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default weight configuration
# ---------------------------------------------------------------------------

DEFAULT_WEIGHTS: Dict[str, int] = {
    "missing_chain": 30,
    "high_degree_supplier": 20,
    "cycle_participation": 30,
    "itc_without_tax_payment": 20,
}

# Category thresholds — upper-bound inclusive
CATEGORY_THRESHOLDS: List[tuple] = [
    (0, "NONE"),
    (20, "LOW"),
    (50, "MEDIUM"),
    (75, "HIGH"),
    (100, "CRITICAL"),
]


def _score_to_category(score: int) -> str:
    """Map a numeric score to a risk category label."""
    for upper, label in CATEGORY_THRESHOLDS:
        if score <= upper:
            return label
    return "CRITICAL"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class InvoiceRiskScore:
    """Scored risk output for a single invoice."""

    invoice_id: str
    invoice_number: str
    supplier_gstin: str
    buyer_gstin: str
    risk_score: int
    risk_category: str
    triggered_rules: List[str] = field(default_factory=list)
    details: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain dict (JSON-safe)."""
        return {
            "invoice_id": self.invoice_id,
            "invoice_number": self.invoice_number,
            "supplier_gstin": self.supplier_gstin,
            "buyer_gstin": self.buyer_gstin,
            "risk_score": self.risk_score,
            "risk_category": self.risk_category,
            "triggered_rules": self.triggered_rules,
            "details": self.details,
        }


@dataclass
class RiskScoringResult:
    """Aggregated output of the risk-scoring engine."""

    scored_invoices: List[InvoiceRiskScore] = field(default_factory=list)
    high_risk_count: int = 0
    critical_count: int = 0
    average_risk_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain dict suitable for JSON response / report."""
        return {
            "scored_invoices": [s.to_dict() for s in self.scored_invoices],
            "high_risk_count": self.high_risk_count,
            "critical_count": self.critical_count,
            "average_risk_score": round(self.average_risk_score, 2),
            "total_flagged": len(self.scored_invoices),
        }


# ---------------------------------------------------------------------------
# RiskScorer
# ---------------------------------------------------------------------------

class RiskScorer:
    """Weighted risk-scoring engine.

    Usage
    -----
    >>> scorer = RiskScorer()
    >>> result = scorer.score(graph, fraud_result)
    """

    def __init__(
        self,
        weights: Optional[Dict[str, int]] = None,
        buyer_gstin: str = "",
    ) -> None:
        """
        Parameters
        ----------
        weights : dict, optional
            Override default rule-to-weight mapping.
        buyer_gstin : str
            The buyer's GSTIN — embedded into each output record.
        """
        self.weights = weights or dict(DEFAULT_WEIGHTS)
        self.buyer_gstin = buyer_gstin

    def score(
        self,
        graph: nx.DiGraph,
        fraud_result: FraudAnalysisResult,
    ) -> RiskScoringResult:
        """Compute per-invoice risk scores.

        Parameters
        ----------
        graph : nx.DiGraph
            Knowledge graph (used to look up invoice attributes).
        fraud_result : FraudAnalysisResult
            Output from ``run_fraud_analysis()``.

        Returns
        -------
        RiskScoringResult
        """
        # Group flags by invoice node ID
        flags_by_invoice: Dict[str, List[FraudFlag]] = {}
        for flag in fraud_result.flags:
            flags_by_invoice.setdefault(flag.invoice_node_id, []).append(flag)

        scored: List[InvoiceRiskScore] = []

        for inv_nid, flags in flags_by_invoice.items():
            inv_data = graph.nodes.get(inv_nid, {})
            triggered_rules: Set[str] = set()
            details: List[Dict[str, Any]] = []
            raw_score = 0

            for flag in flags:
                rule = flag.rule_name
                if rule in triggered_rules:
                    # Don't double-count the same rule on the same invoice
                    continue
                triggered_rules.add(rule)
                weight = self.weights.get(rule, 0)
                raw_score += weight
                details.append({
                    "rule": rule,
                    "weight": weight,
                    "severity": flag.severity,
                    "description": flag.description,
                    "metadata": flag.metadata,
                })

            # Clamp to [0, 100]
            clamped_score = min(max(raw_score, 0), 100)

            scored.append(InvoiceRiskScore(
                invoice_id=inv_nid,
                invoice_number=inv_data.get("invoice_number", ""),
                supplier_gstin=inv_data.get("supplier_gstin", ""),
                buyer_gstin=self.buyer_gstin,
                risk_score=clamped_score,
                risk_category=_score_to_category(clamped_score),
                triggered_rules=sorted(triggered_rules),
                details=details,
            ))

        # Sort descending by risk score for easy consumption
        scored.sort(key=lambda s: s.risk_score, reverse=True)

        high_risk = sum(1 for s in scored if s.risk_category == "HIGH")
        critical = sum(1 for s in scored if s.risk_category == "CRITICAL")
        avg = (
            sum(s.risk_score for s in scored) / len(scored) if scored else 0.0
        )

        result = RiskScoringResult(
            scored_invoices=scored,
            high_risk_count=high_risk,
            critical_count=critical,
            average_risk_score=avg,
        )

        logger.info(
            "Risk scoring complete: %d invoices scored — %d HIGH, %d CRITICAL, avg=%.1f",
            len(scored),
            high_risk,
            critical,
            avg,
        )
        return result


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

def compute_risk_scores(
    graph: nx.DiGraph,
    fraud_result: FraudAnalysisResult,
    *,
    buyer_gstin: str = "",
    weights: Optional[Dict[str, int]] = None,
) -> RiskScoringResult:
    """One-shot convenience wrapper around :class:`RiskScorer`.

    Parameters
    ----------
    graph : nx.DiGraph
    fraud_result : FraudAnalysisResult
    buyer_gstin : str
    weights : dict, optional

    Returns
    -------
    RiskScoringResult
    """
    scorer = RiskScorer(weights=weights, buyer_gstin=buyer_gstin)
    return scorer.score(graph, fraud_result)
