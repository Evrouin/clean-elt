"""
Business rules related exceptions
"""
from typing import Dict, Any, Optional
from .base import ETLException
from src.utils.status_codes import ErrorCode


class BusinessRuleException(ETLException):
    """Base exception for business rule errors"""
    pass


class RuleEvaluationError(BusinessRuleException):
    """Exception raised during rule evaluation"""
    
    def __init__(
        self,
        rule_id: str,
        rule_type: str,
        expression: str = None,
        row_data: dict = None,
        original_exception: Optional[Exception] = None
    ):
        context = {
            'rule_id': rule_id,
            'rule_type': rule_type,
            'expression': expression,
            'row_data': str(row_data)[:200] + '...' if row_data and len(str(row_data)) > 200 else row_data
        }
        
        # Map rule type to specific error code
        error_code_map = {
            'RANGE': ErrorCode.RULE_504,
            'COMPARISON': ErrorCode.RULE_505,
            'BUSINESS': ErrorCode.RULE_506
        }
        
        error_code = error_code_map.get(rule_type, ErrorCode.RULE_502)
        
        super().__init__(
            error_code=error_code,
            context=context,
            original_exception=original_exception
        )


class FastFailException(BusinessRuleException):
    """Exception raised when fast-fail is triggered"""
    
    def __init__(self, rule_id: str, critical_error: str):
        context = {
            'rule_id': rule_id,
            'critical_error': critical_error,
            'fast_fail': True
        }
        super().__init__(
            error_code=ErrorCode.RULE_507,
            context=context
        )


class InvalidRuleExpressionError(BusinessRuleException):
    """Exception raised when rule expression is invalid"""
    
    def __init__(self, rule_id: str, expression: str, rule_type: str):
        context = {
            'rule_id': rule_id,
            'expression': expression,
            'rule_type': rule_type
        }
        super().__init__(
            error_code=ErrorCode.RULE_503,
            context=context
        )
