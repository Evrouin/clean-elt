from typing import Dict, List, Any
from src.models.enums import ReportType


class SchemaValidator:
    """Utility for validating data against Redshift table schemas"""
    
    # Column mappings for each report type
    SCHEMA_MAPPINGS = {
        ReportType.SALES: {
            'transaction_id': 'VARCHAR(50)',
            'product_id': 'VARCHAR(50)', 
            'quantity': 'INTEGER',
            'unit_price': 'DECIMAL(10,2)',
            'total_amount': 'DECIMAL(12,2)',
            'transaction_date': 'DATE'
        },
        ReportType.INVENTORY: {
            'product_id': 'VARCHAR(50)',
            'product_name': 'VARCHAR(255)',
            'category': 'VARCHAR(100)',
            'quantity_on_hand': 'INTEGER',
            'reorder_level': 'INTEGER',
            'last_updated': 'TIMESTAMP'
        },
        ReportType.EXPENSE: {
            'expense_id': 'VARCHAR(50)',
            'employee_id': 'VARCHAR(50)',
            'category': 'VARCHAR(100)',
            'amount': 'DECIMAL(10,2)',
            'expense_date': 'DATE',
            'description': 'VARCHAR(500)'
        }
    }
    
    @classmethod
    def get_table_name(cls, report_type: ReportType) -> str:
        """Get Redshift table name for report type"""
        return f"{report_type.display_name}_reports"
    
    @classmethod
    def get_copy_columns(cls, report_type: ReportType) -> List[str]:
        """Get column list for COPY command"""
        return list(cls.SCHEMA_MAPPINGS[report_type].keys())
    
    @classmethod
    def validate_row_schema(cls, row: Dict[str, Any], report_type: ReportType) -> bool:
        """Validate if row matches expected schema"""
        expected_columns = set(cls.SCHEMA_MAPPINGS[report_type].keys())
        actual_columns = set(row.keys())
        return expected_columns.issubset(actual_columns)
