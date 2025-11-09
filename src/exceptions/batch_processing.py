"""
Batch processing related exceptions
"""
from typing import Dict, Any, Optional, List
from .base import ETLException
from src.utils.status_codes import ErrorCode


class BatchProcessingException(ETLException):
    """Base exception for batch processing errors"""
    pass


class BatchCopyException(BatchProcessingException):
    """Exception raised during batch COPY operations"""
    
    def __init__(
        self,
        operation_type: str,
        failed_files: List[str] = None,
        total_files: int = None,
        original_exception: Optional[Exception] = None
    ):
        context = {
            'operation_type': operation_type,
            'failed_files': failed_files,
            'total_files': total_files,
            'failed_count': len(failed_files) if failed_files else 0
        }
        
        # Map operation type to specific error code
        error_code_map = {
            'batch_copy': ErrorCode.BATCH_701,
            'manifest_copy': ErrorCode.BATCH_702,
            'execution': ErrorCode.BATCH_703
        }
        
        error_code = error_code_map.get(operation_type, ErrorCode.BATCH_701)
        
        super().__init__(
            error_code=error_code,
            context=context,
            original_exception=original_exception
        )


class ManifestException(BatchProcessingException):
    """Exception raised during manifest operations"""
    
    def __init__(
        self,
        manifest_path: str,
        operation: str,
        file_count: int = None,
        original_exception: Optional[Exception] = None
    ):
        context = {
            'manifest_path': manifest_path,
            'operation': operation,
            'file_count': file_count
        }
        super().__init__(
            error_code=ErrorCode.BATCH_705,
            context=context,
            original_exception=original_exception
        )
