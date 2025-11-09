"""
Data processing related exceptions
"""
from typing import Dict, Any, Optional
from .base import ETLException
from src.utils.status_codes import ErrorCode


class DataProcessingException(ETLException):
    """Base exception for data processing errors"""
    pass


class ValidationException(DataProcessingException):
    """Exception raised during data validation"""
    
    def __init__(
        self,
        validation_errors: list,
        total_rows: int = None,
        invalid_rows: int = None,
        original_exception: Optional[Exception] = None
    ):
        context = {
            'validation_errors': validation_errors,
            'total_rows': total_rows,
            'invalid_rows': invalid_rows
        }
        super().__init__(
            error_code=ErrorCode.DATA_407,
            context=context,
            original_exception=original_exception
        )


class UnknownReportTypeError(DataProcessingException):
    """Exception raised when report type cannot be determined"""
    
    def __init__(self, file_key: str, available_types: list = None):
        context = {
            'file_key': file_key,
            'available_types': available_types
        }
        super().__init__(
            error_code=ErrorCode.DATA_405,
            context=context
        )


class ReportProcessingError(DataProcessingException):
    """Exception raised during report processing"""
    
    def __init__(
        self,
        report_type: str,
        file_path: str,
        processing_stage: str = None,
        original_exception: Optional[Exception] = None
    ):
        context = {
            'report_type': report_type,
            'file_path': file_path,
            'processing_stage': processing_stage
        }
        
        # Map report type to specific error code
        error_code_map = {
            'sales': ErrorCode.DATA_401,
            'inventory': ErrorCode.DATA_402,
            'expense': ErrorCode.DATA_403
        }
        
        error_code = error_code_map.get(report_type.lower(), ErrorCode.DATA_404)
        
        super().__init__(
            error_code=error_code,
            context=context,
            original_exception=original_exception
        )
