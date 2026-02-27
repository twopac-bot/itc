import pandas as pd
from typing import Dict, List
import xlsxwriter
from datetime import datetime
import os
from pathlib import Path


class ReportGenerator:
    """Generate Excel reports for reconciliation results"""
    
    @staticmethod
    def generate_excel_report(result: Dict, output_path: str) -> str:
        """Generate comprehensive Excel report"""
        # Create workbook
        workbook = xlsxwriter.Workbook(output_path)
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1,
            'align': 'center',
            'valign': 'vcenter'
        })
        
        currency_format = workbook.add_format({
            'num_format': '₹#,##0.00',
            'border': 1
        })
        
        warning_format = workbook.add_format({
            'bg_color': '#FFE699',
            'border': 1
        })
        
        danger_format = workbook.add_format({
            'bg_color': '#FF9999',
            'border': 1
        })
        
        success_format = workbook.add_format({
            'bg_color': '#C6EFCE',
            'border': 1
        })
        
        border_format = workbook.add_format({'border': 1})
        
        # Add Summary Sheet
        summary_sheet = workbook.add_worksheet('Summary')
        ReportGenerator._write_summary_sheet(summary_sheet, result['summary'], 
                                           header_format, currency_format, border_format)
        
        # Add Matched Invoices Sheet
        if result['matched_invoices']:
            matched_sheet = workbook.add_worksheet('Matched Invoices')
            ReportGenerator._write_matched_sheet(matched_sheet, result['matched_invoices'],
                                               header_format, currency_format, border_format)
        
        # Add Missing in Books Sheet (ITC Opportunities)
        if result['missing_in_books']:
            missing_sheet = workbook.add_worksheet('ITC Opportunities')
            ReportGenerator._write_missing_sheet(missing_sheet, result['missing_in_books'],
                                               header_format, currency_format, 
                                               warning_format, danger_format, border_format)
        
        # Add Pending Vendor Filing Sheet
        if result['pending_vendor_filing']:
            pending_sheet = workbook.add_worksheet('Pending Vendor Filing')
            ReportGenerator._write_pending_sheet(pending_sheet, result['pending_vendor_filing'],
                                               header_format, currency_format, border_format)
        
        # Add Mismatches Sheet
        if result['mismatches']:
            mismatch_sheet = workbook.add_worksheet('Mismatches')
            ReportGenerator._write_mismatch_sheet(mismatch_sheet, result['mismatches'],
                                                header_format, currency_format, 
                                                warning_format, border_format)
        
        # Add Risk Analysis Sheet (from graph intelligence layer)
        graph_analysis = result.get('graph_analysis')
        if graph_analysis and graph_analysis.get('risk_scores'):
            risk_sheet = workbook.add_worksheet('Risk Analysis')
            ReportGenerator._write_risk_analysis_sheet(
                risk_sheet,
                graph_analysis,
                header_format,
                currency_format,
                warning_format,
                danger_format,
                success_format,
                border_format,
            )

        # Add Instructions Sheet
        instructions_sheet = workbook.add_worksheet('Instructions')
        ReportGenerator._write_instructions_sheet(instructions_sheet, header_format, border_format)
        
        workbook.close()
        return output_path
    
    @staticmethod
    def _write_summary_sheet(worksheet, summary: Dict, header_format, currency_format, border_format):
        """Write summary statistics"""
        worksheet.set_column('A:B', 40)
        worksheet.set_column('C:C', 20)
        
        # Title
        worksheet.merge_range('A1:C1', 'GST Reconciliation Summary', header_format)
        worksheet.merge_range('A2:C2', f'Generated on: {datetime.now().strftime("%d-%b-%Y %I:%M %p")}', border_format)
        
        # Summary data
        row = 4
        summary_data = [
            ('Overview', '', ''),
            ('Total Invoices in GSTR-2B', summary['total_invoices_in_gstr2b'], ''),
            ('Total Invoices in Tally Books', summary['total_invoices_in_tally'], ''),
            ('', '', ''),
            ('Matching Results', '', ''),
            ('Matched Invoices', summary['matched_count'], summary['matched_itc_amount']),
            ('Missing in Books (ITC Available)', summary['missing_in_books_count'], summary['available_itc_amount']),
            ('Expired ITC (>180 days)', '', summary['expired_itc_amount']),
            ('Pending Vendor Filing', summary['pending_vendor_filing_count'], summary['pending_itc_amount']),
            ('Invoices with Mismatches', summary['mismatch_count'], ''),
            ('', '', ''),
            ('Alerts', '', ''),
            ('Invoices Near Deadline (>150 days)', summary['deadline_warnings'], ''),
            ('', '', ''),
            ('Total Claimable ITC', '', summary['available_itc_amount']),
        ]
        
        for item, count, amount in summary_data:
            worksheet.write(row, 0, item, header_format if item and not count and not amount else border_format)
            if count != '':
                worksheet.write(row, 1, count, border_format)
            else:
                worksheet.write(row, 1, '', border_format)
            if amount != '':
                worksheet.write(row, 2, amount, currency_format)
            else:
                worksheet.write(row, 2, '', border_format)
            row += 1
    
    @staticmethod
    def _write_matched_sheet(worksheet, matched_invoices: List[Dict], 
                           header_format, currency_format, border_format):
        """Write matched invoices data"""
        headers = [
            'Supplier GSTIN', 'Invoice Number', 'Invoice Date', 
            'Taxable Value', 'IGST', 'CGST', 'SGST', 'Total Tax',
            'Invoice Value', 'Match Type', 'Match Score'
        ]
        
        # Set column widths
        widths = [18, 20, 12, 15, 12, 12, 12, 12, 15, 12, 12]
        for i, width in enumerate(widths):
            worksheet.set_column(i, i, width)
        
        # Write headers
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Write data
        row = 1
        for inv in matched_invoices:
            gstr_inv = inv['gstr2b']
            worksheet.write(row, 0, gstr_inv['supplier_gstin'], border_format)
            worksheet.write(row, 1, gstr_inv['invoice_number'], border_format)
            worksheet.write(row, 2, gstr_inv['invoice_date'], border_format)
            worksheet.write(row, 3, gstr_inv['taxable_value'], currency_format)
            worksheet.write(row, 4, gstr_inv['igst'], currency_format)
            worksheet.write(row, 5, gstr_inv['cgst'], currency_format)
            worksheet.write(row, 6, gstr_inv['sgst'], currency_format)
            worksheet.write(row, 7, gstr_inv['total_tax'], currency_format)
            worksheet.write(row, 8, gstr_inv['total_value'], currency_format)
            worksheet.write(row, 9, inv['match_type'], border_format)
            worksheet.write(row, 10, inv.get('match_score', 100), border_format)
            row += 1
    
    @staticmethod
    def _write_missing_sheet(worksheet, missing_invoices: List[Dict],
                           header_format, currency_format, warning_format, 
                           danger_format, border_format):
        """Write missing invoices (ITC opportunities)"""
        headers = [
            'Supplier GSTIN', 'Invoice Number', 'Invoice Date',
            'Taxable Value', 'IGST', 'CGST', 'SGST', 'Total ITC Available',
            'Invoice Value', 'Days Old', 'Status'
        ]
        
        # Set column widths
        widths = [18, 20, 12, 15, 12, 12, 12, 18, 15, 10, 15]
        for i, width in enumerate(widths):
            worksheet.set_column(i, i, width)
        
        # Write headers
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Write data
        row = 1
        for inv_data in missing_invoices:
            inv = inv_data['invoice']
            days_old = inv_data['days_old']
            
            # Determine format based on status
            if inv_data['expired']:
                row_format = danger_format
                status = 'EXPIRED'
            elif inv_data['deadline_warning']:
                row_format = warning_format
                status = 'CLAIM SOON'
            else:
                row_format = border_format
                status = 'Available'
            
            worksheet.write(row, 0, inv['supplier_gstin'], row_format)
            worksheet.write(row, 1, inv['invoice_number'], row_format)
            worksheet.write(row, 2, inv['invoice_date'], row_format)
            worksheet.write(row, 3, inv['taxable_value'], currency_format if row_format == border_format else row_format)
            worksheet.write(row, 4, inv['igst'], currency_format if row_format == border_format else row_format)
            worksheet.write(row, 5, inv['cgst'], currency_format if row_format == border_format else row_format)
            worksheet.write(row, 6, inv['sgst'], currency_format if row_format == border_format else row_format)
            worksheet.write(row, 7, inv['total_tax'], currency_format if row_format == border_format else row_format)
            worksheet.write(row, 8, inv['total_value'], currency_format if row_format == border_format else row_format)
            worksheet.write(row, 9, days_old, row_format)
            worksheet.write(row, 10, status, row_format)
            row += 1
    
    @staticmethod
    def _write_pending_sheet(worksheet, pending_invoices: List[Dict],
                           header_format, currency_format, border_format):
        """Write pending vendor filing invoices"""
        headers = [
            'Party Name', 'GSTIN', 'Invoice Number', 'Invoice Date',
            'Taxable Value', 'IGST', 'CGST', 'SGST', 'Expected ITC',
            'Invoice Value'
        ]
        
        # Set column widths
        widths = [30, 18, 20, 12, 15, 12, 12, 12, 15, 15]
        for i, width in enumerate(widths):
            worksheet.set_column(i, i, width)
        
        # Write headers
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Write data
        row = 1
        for inv_data in pending_invoices:
            inv = inv_data['invoice']
            worksheet.write(row, 0, inv.get('party_name', ''), border_format)
            worksheet.write(row, 1, inv['gstin'], border_format)
            worksheet.write(row, 2, inv['invoice_number'], border_format)
            worksheet.write(row, 3, inv['date'], border_format)
            worksheet.write(row, 4, inv['taxable_value'], currency_format)
            worksheet.write(row, 5, inv['igst'], currency_format)
            worksheet.write(row, 6, inv['cgst'], currency_format)
            worksheet.write(row, 7, inv['sgst'], currency_format)
            worksheet.write(row, 8, inv['total_tax'], currency_format)
            worksheet.write(row, 9, inv['total_value'], currency_format)
            row += 1
    
    @staticmethod
    def _write_mismatch_sheet(worksheet, mismatches: List[Dict],
                            header_format, currency_format, warning_format, border_format):
        """Write invoice mismatches"""
        headers = [
            'Supplier GSTIN', 'Invoice Number', 'GSTR-2B Date', 'Tally Date',
            'GSTR-2B Value', 'Tally Value', 'Difference', 'Mismatch Details',
            'Match Score'
        ]
        
        # Set column widths
        widths = [18, 20, 12, 12, 15, 15, 12, 40, 12]
        for i, width in enumerate(widths):
            worksheet.set_column(i, i, width)
        
        # Write headers
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        
        # Write data
        row = 1
        for mismatch in mismatches:
            gstr_inv = mismatch['gstr2b']
            tally_inv = mismatch['tally']
            
            worksheet.write(row, 0, gstr_inv['supplier_gstin'], border_format)
            worksheet.write(row, 1, gstr_inv['invoice_number'], border_format)
            worksheet.write(row, 2, gstr_inv['invoice_date'], border_format)
            worksheet.write(row, 3, tally_inv['date'], border_format)
            worksheet.write(row, 4, gstr_inv['total_value'], currency_format)
            worksheet.write(row, 5, tally_inv['total_value'], currency_format)
            worksheet.write(row, 6, abs(gstr_inv['total_value'] - tally_inv['total_value']), warning_format)
            worksheet.write(row, 7, ', '.join(mismatch['mismatches']), border_format)
            worksheet.write(row, 8, mismatch['match_score'], border_format)
            row += 1

    @staticmethod
    def _write_risk_analysis_sheet(
        worksheet, graph_analysis: Dict, header_format, currency_format,
        warning_format, danger_format, success_format, border_format,
    ):
        """Write risk-analysis results produced by the graph intelligence layer."""
        risk_data = graph_analysis.get('risk_scores', {})
        scored_invoices = risk_data.get('scored_invoices', [])
        graph_stats = graph_analysis.get('graph_stats', {})

        # ---- sub-header: aggregate stats ----
        worksheet.set_column('A:A', 30)
        worksheet.set_column('B:B', 20)
        worksheet.merge_range('A1:H1', 'Graph Intelligence — Risk Analysis', header_format)

        meta_rows = [
            ('Total Flagged Invoices', risk_data.get('total_flagged', 0)),
            ('High-Risk Invoices', risk_data.get('high_risk_count', 0)),
            ('Critical Invoices', risk_data.get('critical_count', 0)),
            ('Average Risk Score', risk_data.get('average_risk_score', 0)),
            ('Graph Nodes', graph_stats.get('total_nodes', 0)),
            ('Graph Edges', graph_stats.get('total_edges', 0)),
        ]
        for idx, (label, value) in enumerate(meta_rows):
            worksheet.write(2 + idx, 0, label, border_format)
            worksheet.write(2 + idx, 1, value, border_format)

        if not scored_invoices:
            worksheet.write(9, 0, 'No risk flags detected — all invoices clean.', success_format)
            return

        # ---- detail table ----
        detail_start = 2 + len(meta_rows) + 2  # blank row gap
        headers = [
            'Invoice Number', 'Supplier GSTIN', 'Buyer GSTIN',
            'Risk Score', 'Risk Category', 'Triggered Rules', 'Description',
        ]
        widths = [22, 20, 20, 14, 16, 30, 60]
        for i, w in enumerate(widths):
            worksheet.set_column(i, i, w)

        for col, h in enumerate(headers):
            worksheet.write(detail_start, col, h, header_format)

        for row_idx, inv in enumerate(scored_invoices, start=detail_start + 1):
            cat = inv.get('risk_category', '')
            if cat == 'CRITICAL':
                fmt = danger_format
            elif cat == 'HIGH':
                fmt = warning_format
            elif cat in ('MEDIUM', 'LOW'):
                fmt = border_format
            else:
                fmt = success_format

            descriptions = '; '.join(
                d.get('description', '') for d in inv.get('details', [])
            )

            worksheet.write(row_idx, 0, inv.get('invoice_number', ''), fmt)
            worksheet.write(row_idx, 1, inv.get('supplier_gstin', ''), fmt)
            worksheet.write(row_idx, 2, inv.get('buyer_gstin', ''), fmt)
            worksheet.write(row_idx, 3, inv.get('risk_score', 0), fmt)
            worksheet.write(row_idx, 4, cat, fmt)
            worksheet.write(row_idx, 5, ', '.join(inv.get('triggered_rules', [])), fmt)
            worksheet.write(row_idx, 6, descriptions, fmt)

    @staticmethod
    def _write_instructions_sheet(worksheet, header_format, border_format):
        """Write instructions for using the report"""
        worksheet.set_column('A:A', 80)
        
        instructions = [
            ('Understanding This Report', True),
            ('', False),
            ('1. Summary Sheet:', True),
            ('   - Overview of reconciliation results', False),
            ('   - Total claimable ITC amount highlighted', False),
            ('   - Alert count for invoices nearing deadline', False),
            ('', False),
            ('2. ITC Opportunities Sheet:', True),
            ('   - Invoices present in GSTR-2B but missing in your books', False),
            ('   - Yellow highlight: Claim soon (>150 days old)', False),
            ('   - Red highlight: Expired (>180 days old, cannot claim)', False),
            ('', False),
            ('3. Matched Invoices Sheet:', True),
            ('   - Successfully matched invoices between GSTR-2B and Tally', False),
            ('   - Shows match type (exact or fuzzy) and match score', False),
            ('', False),
            ('4. Pending Vendor Filing Sheet:', True),
            ('   - Invoices in your books but not in GSTR-2B', False),
            ('   - Follow up with vendors to ensure they file returns', False),
            ('', False),
            ('5. Mismatches Sheet:', True),
            ('   - Invoices with amount or date differences', False),
            ('   - Review and correct in books if needed', False),
            ('', False),
            ('6. Risk Analysis Sheet (NEW):', True),
            ('   - Graph-based fraud detection and risk scoring', False),
            ('   - Each flagged invoice has a risk score (0-100)', False),
            ('   - CRITICAL (red): carousel fraud or phantom invoices', False),
            ('   - HIGH (yellow): missing chain or cycle participation', False),
            ('   - Rules: missing_chain, high_degree_supplier, cycle_participation', False),
            ('', False),
            ('Action Items:', True),
            ('1. Immediately claim ITC for yellow highlighted invoices', False),
            ('2. Record missing invoices in your books', False),
            ('3. Follow up with vendors for pending filings', False),
            ('4. Review and resolve mismatches', False),
            ('5. Investigate CRITICAL and HIGH risk invoices in Risk Analysis sheet', False),
        ]
        
        row = 0
        for text, is_header in instructions:
            if is_header:
                worksheet.write(row, 0, text, header_format)
            else:
                worksheet.write(row, 0, text, border_format)
            row += 1