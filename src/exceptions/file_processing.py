"""
File processing related exceptions
"""
from typing import Dict, Any, Optional
from .base import ETLException
from src.utils.status_codes import ErrorCode


class FileProcessingException(ETLException):
    """Base exception for file processing errors"""
    pass


class FileNotFoundError(FileProcessingException):
    """Exception raised when file is not found"""
    
    def __init__(self, file_path: str, original_exception: Optional[Exception] = None):
        context = {'file_path': file_path}
        super().__init__(
            error_code=ErrorCode.FILE_005,
            context=context,
            original_exception=original_exception
        )


class InvalidFileFormatError(FileProcessingException):
    """Exception raised when file format is invalid"""
    
    def __init__(self, file_path: str, expected_format: str, actual_format: str = None):
        context = {
            'file_path': file_path,
            'expected_format': expected_format,
            'actual_format': actual_format
        }
        super().__init__(
            error_code=ErrorCode.FILE_004,
            context=context
        )


class CSVStreamingError(FileProcessingException):
    """Exception raised during CSV streaming operations"""
    
    def __init__(self, file_path: str, row_number: int = None, original_exception: Optional[Exception] = None):
        context = {
            'file_path': file_path,
            'row_number': row_number
        }
        super().__init__(
            error_code=ErrorCode.FILE_001,
            context=context,
            original_exception=original_exception
        )
