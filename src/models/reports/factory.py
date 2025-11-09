from src.processors.sales import SalesProcessor
from src.processors.inventory import InventoryProcessor
from src.processors.expense import ExpenseProcessor
from src.processors.base import BaseProcessor
from src.models.enums import ReportType


class ReportFactory:
    """Factory class for creating report processors"""

    PROCESSORS = {
        ReportType.SALES: SalesProcessor,
        ReportType.INVENTORY: InventoryProcessor,
        ReportType.EXPENSE: ExpenseProcessor,
    }

    @staticmethod
    def create_processor(s3_key: str) -> BaseProcessor:
        """Create appropriate processor based on S3 key"""
        report_type = ReportType.from_s3_key(s3_key)
        processor_class = ReportFactory.PROCESSORS.get(report_type)

        if processor_class:
            return processor_class()

        raise ValueError(f"No processor found for report type: {report_type}")
