import json
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import re
from pathlib import Path


class GSTRParser:
    """Parser for GSTR-2B JSON/Excel files"""
    
    @staticmethod
    def parse_gstr2b(file_path: str) -> Dict:
        """Parse GSTR-2B file (JSON or Excel format)"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.json':
            return GSTRParser._parse_gstr2b_json(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            return GSTRParser._parse_gstr2b_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
    
    @staticmethod
    def _parse_gstr2b_json(file_path: str) -> Dict:
        """Parse GSTR-2B JSON file"""
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        invoices = []
        gstin = data.get('gstin', '')
        fp = data.get('fp', '')
        
        # Parse B2B invoices
        for supplier in data.get('b2b', []):
            supplier_gstin = supplier.get('ctin', '')
            
            for inv in supplier.get('inv', []):
                invoice_num = inv.get('inum', '')
                invoice_date = inv.get('idt', '')
                total_value = inv.get('val', 0)
                
                # Calculate tax amounts
                igst = 0
                cgst = 0
                sgst = 0
                taxable_value = 0
                
                for item in inv.get('itms', []):
                    taxable_value += item.get('txval', 0)
                    igst += item.get('igst', 0)
                    cgst += item.get('cgst', 0)
                    sgst += item.get('sgst', 0)
                
                invoices.append({
                    'supplier_gstin': supplier_gstin,
                    'invoice_number': invoice_num,
                    'invoice_date': invoice_date,
                    'taxable_value': taxable_value,
                    'igst': igst,
                    'cgst': cgst,
                    'sgst': sgst,
                    'total_value': total_value,
                    'total_tax': igst + cgst + sgst
                })
        
        return {
            'gstin': gstin,
            'filing_period': fp,
            'invoices': invoices
        }
    
    @staticmethod
    def _parse_gstr2b_excel(file_path: str) -> Dict:
        """Parse GSTR-2B Excel file"""
        df = pd.read_excel(file_path, sheet_name=None)
        
        invoices = []
        gstin = ''
        fp = ''
        
        # Find B2B sheet
        b2b_sheet = None
        for sheet_name, sheet_df in df.items():
            if 'b2b' in sheet_name.lower():
                b2b_sheet = sheet_df
                break
        
        if b2b_sheet is None:
            raise ValueError("No B2B sheet found in Excel file")
        
        # Parse invoices from B2B sheet
        for _, row in b2b_sheet.iterrows():
            if pd.notna(row.get('Invoice Number', row.get('Invoice No.', ''))):
                invoices.append({
                    'supplier_gstin': str(row.get('GSTIN of supplier', row.get('Supplier GSTIN', ''))),
                    'invoice_number': str(row.get('Invoice Number', row.get('Invoice No.', ''))),
                    'invoice_date': str(row.get('Invoice Date', row.get('Date', ''))),
                    'taxable_value': float(row.get('Taxable Value', 0)),
                    'igst': float(row.get('Integrated Tax', row.get('IGST', 0))),
                    'cgst': float(row.get('Central Tax', row.get('CGST', 0))),
                    'sgst': float(row.get('State Tax', row.get('SGST', 0))),
                    'total_value': float(row.get('Invoice Value', row.get('Total', 0))),
                    'total_tax': float(row.get('Integrated Tax', 0)) + float(row.get('Central Tax', 0)) + float(row.get('State Tax', 0))
                })
        
        return {
            'gstin': gstin,
            'filing_period': fp,
            'invoices': invoices
        }


class TallyParser:
    """Parser for Tally export files"""
    
    @staticmethod
    def parse_tally_data(file_path: str) -> List[Dict]:
        """Parse Tally export file (Excel/CSV)"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(file_path)
        elif file_ext == '.csv':
            df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
        
        # Normalize column names
        df.columns = df.columns.str.strip()
        
        invoices = []
        
        # Common column name variations
        date_cols = ['Date', 'Invoice Date', 'Voucher Date', 'Vch Date']
        party_cols = ['Party Name', 'Particulars', 'Ledger Name', 'Account Name']
        gstin_cols = ['GSTIN/UIN', 'GSTIN', 'GST No.', 'GST Number']
        invoice_cols = ['Invoice No.', 'Vch No.', 'Invoice Number', 'Voucher No.', 'Bill No.']
        taxable_cols = ['Taxable Value', 'Taxable Amount', 'Basic Amount', 'Assessable Value']
        igst_cols = ['IGST @ 18%', 'Integrated Tax Amount', 'IGST', 'IGST Amount']
        cgst_cols = ['CGST', 'Central Tax Amount', 'CGST Amount']
        sgst_cols = ['SGST', 'State Tax Amount', 'SGST Amount']
        total_cols = ['Total', 'Invoice Value', 'Total Amount', 'Gross Total']
        itc_claimed_cols = ['ITC Claimed', 'ITC Availed', 'Input Tax Credit Claimed']
        
        # Find actual column names
        date_col = next((col for col in date_cols if col in df.columns), None)
        party_col = next((col for col in party_cols if col in df.columns), None)
        gstin_col = next((col for col in gstin_cols if col in df.columns), None)
        invoice_col = next((col for col in invoice_cols if col in df.columns), None)
        taxable_col = next((col for col in taxable_cols if col in df.columns), None)
        igst_col = next((col for col in igst_cols if col in df.columns), None)
        cgst_col = next((col for col in cgst_cols if col in df.columns), None)
        sgst_col = next((col for col in sgst_cols if col in df.columns), None)
        total_col = next((col for col in total_cols if col in df.columns), None)
        itc_claimed_col = next((col for col in itc_claimed_cols if col in df.columns), None)
        
        # Parse each row
        for _, row in df.iterrows():
            # Skip empty rows
            if pd.isna(row.get(invoice_col, '')):
                continue
            
            # Parse ITC claimed status
            itc_claimed = False
            if itc_claimed_col and itc_claimed_col in row:
                itc_value = str(row[itc_claimed_col]).lower()
                itc_claimed = itc_value in ['yes', 'y', '1', 'true', 'claimed']
            
            invoices.append({
                'date': str(row.get(date_col, '')) if date_col else '',
                'party_name': str(row.get(party_col, '')) if party_col else '',
                'gstin': TallyParser._clean_gstin(str(row.get(gstin_col, '')) if gstin_col else ''),
                'invoice_number': TallyParser._clean_invoice_number(str(row.get(invoice_col, '')) if invoice_col else ''),
                'taxable_value': float(row.get(taxable_col, 0)) if taxable_col else 0,
                'igst': float(row.get(igst_col, 0)) if igst_col else 0,
                'cgst': float(row.get(cgst_col, 0)) if cgst_col else 0,
                'sgst': float(row.get(sgst_col, 0)) if sgst_col else 0,
                'total_value': float(row.get(total_col, 0)) if total_col else 0,
                'total_tax': (float(row.get(igst_col, 0)) if igst_col else 0) + 
                            (float(row.get(cgst_col, 0)) if cgst_col else 0) + 
                            (float(row.get(sgst_col, 0)) if sgst_col else 0),
                'itc_claimed': itc_claimed
            })
        
        return invoices
    
    @staticmethod
    def _clean_gstin(gstin: str) -> str:
        """Clean and normalize GSTIN"""
        if pd.isna(gstin) or gstin == 'nan':
            return ''
        # Remove spaces and convert to uppercase
        gstin = gstin.strip().upper().replace(' ', '')
        # Remove any non-alphanumeric characters
        gstin = re.sub(r'[^A-Z0-9]', '', gstin)
        return gstin
    
    @staticmethod
    def _clean_invoice_number(invoice_num: str) -> str:
        """Clean and normalize invoice number"""
        if pd.isna(invoice_num) or invoice_num == 'nan':
            return ''
        # Convert to string and strip
        invoice_num = str(invoice_num).strip().upper()
        # Remove common separators but keep the structure
        invoice_num = invoice_num.replace(' ', '').replace('/', '-')
        return invoice_num