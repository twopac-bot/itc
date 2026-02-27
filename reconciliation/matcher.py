from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
import re
from dataclasses import dataclass
from rapidfuzz import fuzz


@dataclass
class MatchResult:
    """Result of invoice matching"""
    matched_invoices: List[Dict]
    missing_in_books: List[Dict]
    pending_vendor_filing: List[Dict]
    mismatches: List[Dict]
    summary: Dict


class InvoiceMatcher:
    """Intelligent invoice matching with fuzzy logic"""
    
    def __init__(self, fuzzy_threshold: int = 80, amount_tolerance: float = 10):
        self.fuzzy_threshold = fuzzy_threshold
        self.amount_tolerance = amount_tolerance
    
    def reconcile(self, gstr2b_data: Dict, tally_data: List[Dict]) -> MatchResult:
        """Main reconciliation logic"""
        gstr2b_invoices = gstr2b_data['invoices']
        
        # Filter out already claimed ITC if applicable
        tally_unclaimed = [inv for inv in tally_data if not inv.get('itc_claimed', False)]
        
        # Create lookup indices
        gstr2b_index = self._create_invoice_index(gstr2b_invoices, 'supplier_gstin')
        tally_index = self._create_invoice_index(tally_unclaimed, 'gstin')
        
        matched_invoices = []
        missing_in_books = []
        pending_vendor_filing = []
        mismatches = []
        
        # Track matched invoices
        matched_gstr2b_ids = set()
        matched_tally_ids = set()
        
        # First pass: Exact matches
        for gstr_inv in gstr2b_invoices:
            match_found = False
            gstin_key = gstr_inv['supplier_gstin']
            
            if gstin_key in tally_index:
                for tally_inv in tally_index[gstin_key]:
                    if self._is_exact_match(gstr_inv, tally_inv):
                        matched_invoices.append({
                            'gstr2b': gstr_inv,
                            'tally': tally_inv,
                            'match_type': 'exact',
                            'itc_amount': gstr_inv['total_tax']
                        })
                        matched_gstr2b_ids.add(id(gstr_inv))
                        matched_tally_ids.add(id(tally_inv))
                        match_found = True
                        break
            
            if not match_found:
                # Try fuzzy matching
                fuzzy_match = self._find_fuzzy_match(gstr_inv, tally_unclaimed)
                if fuzzy_match:
                    tally_inv, score = fuzzy_match
                    if id(tally_inv) not in matched_tally_ids:
                        # Check for mismatches
                        mismatch_details = self._get_mismatch_details(gstr_inv, tally_inv)
                        if mismatch_details:
                            mismatches.append({
                                'gstr2b': gstr_inv,
                                'tally': tally_inv,
                                'match_score': score,
                                'mismatches': mismatch_details,
                                'itc_amount': gstr_inv['total_tax']
                            })
                        else:
                            matched_invoices.append({
                                'gstr2b': gstr_inv,
                                'tally': tally_inv,
                                'match_type': 'fuzzy',
                                'match_score': score,
                                'itc_amount': gstr_inv['total_tax']
                            })
                        matched_gstr2b_ids.add(id(gstr_inv))
                        matched_tally_ids.add(id(tally_inv))
        
        # Identify missing in books (ITC opportunities)
        for gstr_inv in gstr2b_invoices:
            if id(gstr_inv) not in matched_gstr2b_ids:
                days_old = self._calculate_days_old(gstr_inv['invoice_date'])
                missing_in_books.append({
                    'invoice': gstr_inv,
                    'itc_amount': gstr_inv['total_tax'],
                    'days_old': days_old,
                    'deadline_warning': days_old > 150,
                    'expired': days_old > 180
                })
        
        # Identify pending vendor filing
        for tally_inv in tally_unclaimed:
            if id(tally_inv) not in matched_tally_ids:
                pending_vendor_filing.append({
                    'invoice': tally_inv,
                    'expected_itc': tally_inv['total_tax']
                })
        
        # Calculate summary
        summary = self._calculate_summary(
            matched_invoices, missing_in_books, 
            pending_vendor_filing, mismatches
        )
        
        return MatchResult(
            matched_invoices=matched_invoices,
            missing_in_books=missing_in_books,
            pending_vendor_filing=pending_vendor_filing,
            mismatches=mismatches,
            summary=summary
        )
    
    def _create_invoice_index(self, invoices: List[Dict], gstin_field: str) -> Dict[str, List[Dict]]:
        """Create index by GSTIN for faster lookup"""
        index = {}
        for inv in invoices:
            gstin = inv.get(gstin_field, '')
            if gstin:
                if gstin not in index:
                    index[gstin] = []
                index[gstin].append(inv)
        return index
    
    def _is_exact_match(self, gstr_inv: Dict, tally_inv: Dict) -> bool:
        """Check if two invoices are exact matches"""
        # Normalize invoice numbers
        gstr_num = self._normalize_invoice_number(gstr_inv['invoice_number'])
        tally_num = self._normalize_invoice_number(tally_inv['invoice_number'])
        
        if gstr_num != tally_num:
            return False
        
        # Check amount within tolerance
        amount_diff = abs(gstr_inv['total_value'] - tally_inv['total_value'])
        return amount_diff <= self.amount_tolerance
    
    def _find_fuzzy_match(self, gstr_inv: Dict, tally_invoices: List[Dict]) -> Optional[Tuple[Dict, int]]:
        """Find best fuzzy match for an invoice"""
        best_match = None
        best_score = 0
        
        gstr_num = self._normalize_invoice_number(gstr_inv['invoice_number'])
        
        for tally_inv in tally_invoices:
            # Skip if GSTIN doesn't match at all
            if gstr_inv['supplier_gstin'] != tally_inv.get('gstin', ''):
                continue
            
            tally_num = self._normalize_invoice_number(tally_inv['invoice_number'])
            
            # Calculate fuzzy score using rapidfuzz
            score = fuzz.ratio(gstr_num, tally_num)
            
            # Consider amount similarity
            amount_diff = abs(gstr_inv['total_value'] - tally_inv['total_value'])
            if amount_diff <= self.amount_tolerance:
                score += 20  # Boost score for matching amounts
            
            if score > best_score and score >= self.fuzzy_threshold:
                best_score = score
                best_match = tally_inv
        
        return (best_match, best_score) if best_match else None
    
    def _normalize_invoice_number(self, invoice_num: str) -> str:
        """Normalize invoice number for comparison"""
        # Remove special characters and convert to uppercase
        normalized = re.sub(r'[^A-Z0-9]', '', str(invoice_num).upper())
        return normalized
    
    def _get_mismatch_details(self, gstr_inv: Dict, tally_inv: Dict) -> List[str]:
        """Get details of mismatches between invoices"""
        mismatches = []
        
        # Check amount mismatch
        amount_diff = abs(gstr_inv['total_value'] - tally_inv['total_value'])
        if amount_diff > self.amount_tolerance:
            mismatches.append(f"Amount difference: ₹{amount_diff:.2f}")
        
        # Check date mismatch
        gstr_date = self._parse_date(gstr_inv['invoice_date'])
        tally_date = self._parse_date(tally_inv['date'])
        
        if gstr_date and tally_date and gstr_date != tally_date:
            days_diff = abs((gstr_date - tally_date).days)
            mismatches.append(f"Date difference: {days_diff} days")
        
        # Check tax component mismatches
        tax_diff = abs(gstr_inv['total_tax'] - tally_inv['total_tax'])
        if tax_diff > self.amount_tolerance:
            mismatches.append(f"Tax difference: ₹{tax_diff:.2f}")
        
        return mismatches
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object"""
        if not date_str or date_str == 'nan':
            return None
        
        # Try common date formats
        formats = [
            '%d-%m-%Y', '%d/%m/%Y', '%Y-%m-%d',
            '%d-%m-%y', '%d/%m/%y', '%d.%m.%Y',
            '%d-%b-%Y', '%d-%B-%Y'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except ValueError:
                continue
        
        return None
    
    def _calculate_days_old(self, invoice_date: str) -> int:
        """Calculate how many days old an invoice is"""
        invoice_dt = self._parse_date(invoice_date)
        if invoice_dt:
            return (datetime.now() - invoice_dt).days
        return 0
    
    def _calculate_summary(self, matched: List[Dict], missing: List[Dict], 
                          pending: List[Dict], mismatches: List[Dict]) -> Dict:
        """Calculate summary statistics"""
        total_matched_itc = sum(inv['itc_amount'] for inv in matched)
        total_available_itc = sum(inv['itc_amount'] for inv in missing if not inv['expired'])
        total_expired_itc = sum(inv['itc_amount'] for inv in missing if inv['expired'])
        total_pending_itc = sum(inv['expected_itc'] for inv in pending)
        
        return {
            'total_invoices_in_gstr2b': len(matched) + len(missing) + len(mismatches),
            'total_invoices_in_tally': len(matched) + len(pending) + len(mismatches),
            'matched_count': len(matched),
            'matched_itc_amount': round(total_matched_itc, 2),
            'missing_in_books_count': len(missing),
            'available_itc_amount': round(total_available_itc, 2),
            'expired_itc_amount': round(total_expired_itc, 2),
            'pending_vendor_filing_count': len(pending),
            'pending_itc_amount': round(total_pending_itc, 2),
            'mismatch_count': len(mismatches),
            'deadline_warnings': sum(1 for inv in missing if inv['deadline_warning'] and not inv['expired'])
        }