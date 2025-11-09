from enum import Enum


class ReportType(Enum):
    """Enum for report types with their S3 path mappings"""
    SALES = ("Reports/Sales/", "sales")
    INVENTORY = ("Reports/Inventory/", "inventory")
    EXPENSE = ("Reports/Expense/", "expense")

    def __init__(self, s3_path: str, display_name: str):
        self.s3_path = s3_path
        self.display_name = display_name

    @classmethod
    def from_s3_key(cls, s3_key: str) -> 'ReportType':
        """Get report type from S3 key"""
        for report_type in cls:
            if report_type.s3_path in s3_key:
                return report_type
        raise ValueError(f"Unknown report type for key: {s3_key}")
