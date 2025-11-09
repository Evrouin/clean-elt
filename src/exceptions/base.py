"""
Base exception classes for CleanELT
"""
from typing import Dict, Any, Optional
from src.utils.status_codes import ErrorCode, WarningCode


class ETLException(Exception):
    """Base exception for all ETL operations"""
    
    def __init__(
        self,
        error_code: ErrorCode,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        self.error_code = error_code
        self.context = context or {}
        self.original_exception = original_exception
        
        if message:
            self.message = f"[{error_code.value}] {message}"
        else:
            from src.utils.status_codes import ErrorMessages
            self.message = ErrorMessages.get_error_message(error_code)
        
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging"""
        return {
            'error_code': self.error_code.value,
            'message': self.message,
            'context': self.context,
            'original_error': str(self.original_exception) if self.original_exception else None
        }


class ETLWarning(UserWarning):
    """Base warning for all ETL operations"""
    
    def __init__(
        self,
        warning_code: WarningCode,
        message: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ):
        self.warning_code = warning_code
        self.context = context or {}
        
        if message:
            self.message = f"[{warning_code.value}] {message}"
        else:
            from src.utils.status_codes import ErrorMessages
            self.message = ErrorMessages.get_warning_message(warning_code)
        
        super().__init__(self.message)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert warning to dictionary for logging"""
        return {
            'warning_code': self.warning_code.value,
            'message': self.message,
            'context': self.context
        }
