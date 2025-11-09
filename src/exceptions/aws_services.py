"""
AWS services related exceptions
"""
from typing import Dict, Any, Optional
from .base import ETLException
from src.utils.status_codes import ErrorCode


class AWSServiceException(ETLException):
    """Base exception for AWS service errors"""
    pass


class S3Exception(AWSServiceException):
    """Exception for S3 operations"""
    
    def __init__(
        self,
        error_code: ErrorCode,
        bucket: str = None,
        key: str = None,
        operation: str = None,
        original_exception: Optional[Exception] = None
    ):
        context = {
            'bucket': bucket,
            'key': key,
            'operation': operation,
            'service': 's3'
        }
        super().__init__(
            error_code=error_code,
            context=context,
            original_exception=original_exception
        )


class RedshiftException(AWSServiceException):
    """Exception for Redshift operations"""
    
    def __init__(
        self,
        error_code: ErrorCode,
        table_name: str = None,
        query: str = None,
        operation: str = None,
        original_exception: Optional[Exception] = None
    ):
        context = {
            'table_name': table_name,
            'query': query[:100] + '...' if query and len(query) > 100 else query,
            'operation': operation,
            'service': 'redshift'
        }
        super().__init__(
            error_code=error_code,
            context=context,
            original_exception=original_exception
        )
