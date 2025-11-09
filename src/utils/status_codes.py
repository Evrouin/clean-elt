"""
Error codes and standardized error messages for ETL Data Quality Checker
"""
from enum import Enum
from typing import Dict, Any


class InfoCode(Enum):
    """Standardized info codes for the ETL Data Quality Checker"""
    
    # General Info Codes (100-199)
    INFO_100 = "INFO_100"  # General informational log
    INFO_101 = "INFO_101"  # Initializing process
    INFO_102 = "INFO_102"  # In progress operation
    INFO_103 = "INFO_103"  # Waiting for dependency
    INFO_104 = "INFO_104"  # Retrying operation
    INFO_105 = "INFO_105"  # Step skipped


class SuccessCode(Enum):
    """Standardized success codes for the ETL Data Quality Checker"""
    
    # General Success Codes (200-299)
    SUCCESS_200 = "SUCCESS_200"  # OK / Success - General success
    SUCCESS_201 = "SUCCESS_201"  # Created - New resource created
    SUCCESS_202 = "SUCCESS_202"  # Accepted / Queued - Async processing
    SUCCESS_203 = "SUCCESS_203"  # Processed with Info - Success with notes
    SUCCESS_204 = "SUCCESS_204"  # No Content - Success but no data
    SUCCESS_205 = "SUCCESS_205"  # Reset / Refresh - Success requiring refresh


class ErrorCode(Enum):
    """Standardized error codes for the ETL Data Quality Checker"""
    
    # File Processing Errors (100-199)
    FILE_001 = "FILE_001"  # CSV streaming error
    FILE_002 = "FILE_002"  # Failed to get file size
    FILE_003 = "FILE_003"  # File processing failed
    FILE_004 = "FILE_004"  # Invalid file format
    FILE_005 = "FILE_005"  # File not found
    
    # AWS Service Errors (200-299)
    AWS_201 = "AWS_201"    # S3 access failed
    AWS_202 = "AWS_202"    # Failed to get S3 object
    AWS_203 = "AWS_203"    # Failed to put S3 object
    AWS_204 = "AWS_204"    # Failed to delete S3 object
    AWS_205 = "AWS_205"    # Failed to copy S3 object
    AWS_206 = "AWS_206"    # Failed to list S3 objects
    AWS_207 = "AWS_207"    # Failed to get S3 object metadata
    
    # Redshift Errors (300-399)
    RS_301 = "RS_301"      # Failed to create connection pool
    RS_302 = "RS_302"      # COPY command failed
    RS_303 = "RS_303"      # Query execution failed
    RS_304 = "RS_304"      # Manifest COPY failed
    RS_305 = "RS_305"      # Redshift integration failed
    RS_306 = "RS_306"      # Failed to log audit record
    RS_307 = "RS_307"      # Connection pool close error
    
    # Data Processing Errors (400-499)
    DATA_401 = "DATA_401"  # Sales processing failed
    DATA_402 = "DATA_402"  # Inventory processing failed
    DATA_403 = "DATA_403"  # Expense processing failed
    DATA_404 = "DATA_404"  # Report processing failed
    DATA_405 = "DATA_405"  # Unknown report type
    DATA_406 = "DATA_406"  # Invalid data format
    DATA_407 = "DATA_407"  # Data validation failed
    
    # Business Rules Errors (500-599)
    RULE_501 = "RULE_501"  # Rule execution failed
    RULE_502 = "RULE_502"  # Rule evaluation failed
    RULE_503 = "RULE_503"  # Invalid range expression
    RULE_504 = "RULE_504"  # Range rule evaluation error
    RULE_505 = "RULE_505"  # Comparison rule evaluation error
    RULE_506 = "RULE_506"  # Expression rule evaluation error
    RULE_507 = "RULE_507"  # Fast-fail triggered
    
    # Handler/Request Errors (600-699)
    REQ_601 = "REQ_601"    # Failed to parse SQS request
    REQ_602 = "REQ_602"    # Invalid SQS event format
    REQ_603 = "REQ_603"    # Handler error occurred
    REQ_604 = "REQ_604"    # Request processing failed
    
    # Batch Processing Errors (700-799)
    BATCH_701 = "BATCH_701"  # Batch COPY failed
    BATCH_702 = "BATCH_702"  # Batch COPY with manifest failed
    BATCH_703 = "BATCH_703"  # Batch COPY execution failed
    BATCH_704 = "BATCH_704"  # Batch manifest processing failed
    BATCH_705 = "BATCH_705"  # Manifest COPY failed
    
    # System/Configuration Errors (800-899)
    SYS_801 = "SYS_801"    # Configuration error
    SYS_802 = "SYS_802"    # Environment variable missing
    SYS_803 = "SYS_803"    # Service initialization failed
    SYS_804 = "SYS_804"    # Resource allocation failed
    SYS_805 = "SYS_805"    # Memory limit exceeded


class WarningCode(Enum):
    """Standardized warning codes for the ETL Data Quality Checker"""
    
    # File Processing Warnings (100-199)
    FILE_W101 = "FILE_W101"  # S3 access failed (using fallback)
    FILE_W102 = "FILE_W102"  # Skipping non-dict row
    FILE_W103 = "FILE_W103"  # File format inconsistency
    
    # Data Processing Warnings (400-499)
    DATA_W401 = "DATA_W401"  # Unknown report type for key
    DATA_W402 = "DATA_W402"  # Skipping unknown report type
    DATA_W403 = "DATA_W403"  # Data quality threshold exceeded
    
    # Business Rules Warnings (500-599)
    RULE_W501 = "RULE_W501"  # Fast-fail triggered on critical rule
    RULE_W502 = "RULE_W502"  # Invalid range expression format
    RULE_W503 = "RULE_W503"  # Rule performance degradation
    
    # System Warnings (800-899)
    SYS_W801 = "SYS_W801"   # Connection pool close error
    SYS_W802 = "SYS_W802"   # Performance threshold exceeded
    SYS_W803 = "SYS_W803"   # Resource usage high


class ErrorMessages:
    """Standardized error message templates"""
    
    INFO_TEMPLATES = {
        InfoCode.INFO_100: "Information log entry",
        InfoCode.INFO_101: "Process initializing",
        InfoCode.INFO_102: "Operation in progress",
        InfoCode.INFO_103: "Waiting for dependent process or input",
        InfoCode.INFO_104: "Retrying operation",
        InfoCode.INFO_105: "Step skipped as per configuration",
    }
    
    SUCCESS_TEMPLATES = {
        SuccessCode.SUCCESS_200: "Operation completed successfully",
        SuccessCode.SUCCESS_201: "Resource created successfully",
        SuccessCode.SUCCESS_202: "Request accepted and is being processed",
        SuccessCode.SUCCESS_203: "Operation completed with additional information",
        SuccessCode.SUCCESS_204: "Operation successful, no content to display",
        SuccessCode.SUCCESS_205: "Operation successful, please refresh or reload",
    }
    
    ERROR_TEMPLATES = {
        # File Processing
        ErrorCode.FILE_001: "CSV streaming error occurred",
        ErrorCode.FILE_002: "Failed to get file size",
        ErrorCode.FILE_003: "File processing failed",
        ErrorCode.FILE_004: "Invalid file format detected",
        ErrorCode.FILE_005: "File not found",
        
        # AWS Services
        ErrorCode.AWS_201: "S3 access failed",
        ErrorCode.AWS_202: "Failed to get S3 object",
        ErrorCode.AWS_203: "Failed to put S3 object",
        ErrorCode.AWS_204: "Failed to delete S3 object",
        ErrorCode.AWS_205: "Failed to copy S3 object",
        ErrorCode.AWS_206: "Failed to list S3 objects",
        ErrorCode.AWS_207: "Failed to get S3 object metadata",
        
        # Redshift
        ErrorCode.RS_301: "Failed to create Redshift connection pool",
        ErrorCode.RS_302: "Redshift COPY command failed",
        ErrorCode.RS_303: "Redshift query execution failed",
        ErrorCode.RS_304: "Redshift manifest COPY failed",
        ErrorCode.RS_305: "Redshift integration failed",
        ErrorCode.RS_306: "Failed to log audit record",
        ErrorCode.RS_307: "Error closing connection pool",
        
        # Data Processing
        ErrorCode.DATA_401: "Sales processing failed",
        ErrorCode.DATA_402: "Inventory processing failed",
        ErrorCode.DATA_403: "Expense processing failed",
        ErrorCode.DATA_404: "Report processing failed",
        ErrorCode.DATA_405: "Unknown report type",
        ErrorCode.DATA_406: "Invalid data format",
        ErrorCode.DATA_407: "Data validation failed",
        
        # Business Rules
        ErrorCode.RULE_501: "Business rule execution failed",
        ErrorCode.RULE_502: "Rule evaluation failed",
        ErrorCode.RULE_503: "Invalid range expression",
        ErrorCode.RULE_504: "Range rule evaluation error",
        ErrorCode.RULE_505: "Comparison rule evaluation error",
        ErrorCode.RULE_506: "Expression rule evaluation error",
        ErrorCode.RULE_507: "Fast-fail triggered",
        
        # Handlers/Requests
        ErrorCode.REQ_601: "Failed to parse SQS request",
        ErrorCode.REQ_602: "Invalid SQS event format",
        ErrorCode.REQ_603: "Handler error occurred",
        ErrorCode.REQ_604: "Request processing failed",
        
        # Batch Processing
        ErrorCode.BATCH_701: "Batch COPY operation failed",
        ErrorCode.BATCH_702: "Batch COPY with manifest failed",
        ErrorCode.BATCH_703: "Batch COPY execution failed",
        ErrorCode.BATCH_704: "Batch manifest processing failed",
        ErrorCode.BATCH_705: "Manifest COPY operation failed",
        
        # System
        ErrorCode.SYS_801: "System configuration error",
        ErrorCode.SYS_802: "Environment variable missing",
        ErrorCode.SYS_803: "Service initialization failed",
        ErrorCode.SYS_804: "Resource allocation failed",
        ErrorCode.SYS_805: "Memory limit exceeded",
    }
    
    WARNING_TEMPLATES = {
        # File Processing
        WarningCode.FILE_W101: "S3 access failed, using fallback",
        WarningCode.FILE_W102: "Skipping non-dict row",
        WarningCode.FILE_W103: "File format inconsistency detected",
        
        # Data Processing
        WarningCode.DATA_W401: "Unknown report type for key",
        WarningCode.DATA_W402: "Skipping unknown report type",
        WarningCode.DATA_W403: "Data quality threshold exceeded",
        
        # Business Rules
        WarningCode.RULE_W501: "Fast-fail triggered on critical rule",
        WarningCode.RULE_W502: "Invalid range expression format",
        WarningCode.RULE_W503: "Rule performance degradation detected",
        
        # System
        WarningCode.SYS_W801: "Connection pool close error",
        WarningCode.SYS_W802: "Performance threshold exceeded",
        WarningCode.SYS_W803: "Resource usage high",
    }
    
    @classmethod
    def get_info_message(cls, info_code: InfoCode, **context) -> str:
        """Get standardized info message with context"""
        base_message = cls.INFO_TEMPLATES.get(info_code, "Information log entry")
        return f"[{info_code.value}] {base_message}"
    
    @classmethod
    def get_success_message(cls, success_code: SuccessCode, **context) -> str:
        """Get standardized success message with context"""
        base_message = cls.SUCCESS_TEMPLATES.get(success_code, "Operation completed")
        return f"[{success_code.value}] {base_message}"
    
    @classmethod
    def get_error_message(cls, error_code: ErrorCode, **context) -> str:
        """Get standardized error message with context"""
        base_message = cls.ERROR_TEMPLATES.get(error_code, "Unknown error")
        return f"[{error_code.value}] {base_message}"
    
    @classmethod
    def get_warning_message(cls, warning_code: WarningCode, **context) -> str:
        """Get standardized warning message with context"""
        base_message = cls.WARNING_TEMPLATES.get(warning_code, "Unknown warning")
        return f"[{warning_code.value}] {base_message}"


def format_error_context(**kwargs) -> Dict[str, Any]:
    """Format error context parameters"""
    return {k: v for k, v in kwargs.items() if v is not None}
