"""
Error logging utility with standardized error codes
"""
from typing import Dict, Any, Optional
from src.utils.logger import StructuredLogger
from src.utils.status_codes import ErrorCode, WarningCode, SuccessCode, InfoCode, ErrorMessages, format_error_context
from src.exceptions.base import ETLException, ETLWarning


class ErrorLogger:
    """Logger with standardized error codes and formatting"""

    def __init__(self, name: str):
        self.logger = StructuredLogger(name)

    def log_info(
        self,
        info_code: InfoCode,
        **context
    ) -> None:
        """Log standardized info with code and context"""
        message = ErrorMessages.get_info_message(info_code)
        formatted_context = format_error_context(**context)

        self.logger.info(
            message,
            info_code=info_code.value,
            **formatted_context,
        )

    def log_success(
        self,
        success_code: SuccessCode,
        **context
    ) -> None:
        """Log standardized success with code and context"""
        message = ErrorMessages.get_success_message(success_code)
        formatted_context = format_error_context(**context)

        self.logger.info(
            message,
            success_code=success_code.value,
            **formatted_context,
        )

    def log_error(
        self,
        error_code: ErrorCode,
        exception: Optional[Exception] = None,
        **context
    ) -> None:
        """Log standardized error with code and context"""
        message = ErrorMessages.get_error_message(error_code)
        formatted_context = format_error_context(**context)

        if exception:
            formatted_context['exception'] = str(exception)
            formatted_context['exception_type'] = type(exception).__name__

        self.logger.error(
            message,
            error_code=error_code.value,
            **formatted_context,
        )

    def log_warning(
        self,
        warning_code: WarningCode,
        **context
    ) -> None:
        """Log standardized warning with code and context"""
        message = ErrorMessages.get_warning_message(warning_code)
        formatted_context = format_error_context(**context)

        self.logger.log_warning(
            warning_code,
            operation="log_warning",
            **formatted_context,
        )

    def log_etl_exception(self, etl_exception: ETLException) -> None:
        """Log ETL exception with full context"""
        exception_data = etl_exception.to_dict()

        self.logger.error(
            exception_data['message'],
            error_code=exception_data['error_code'],
            context=exception_data['context'],
            original_error=exception_data['original_error'],
        )

    def log_etl_warning(self, etl_warning: ETLWarning) -> None:
        """Log ETL warning with full context"""
        warning_data = etl_warning.to_dict()

        self.logger.log_warning(
            WarningCode.SYS_W803,
            operation="log_etl_warning",
            warning_message=warning_data['message'],
            warning_code=warning_data['warning_code'],
            context=warning_data['context'],
        )

    # Convenience methods for common error patterns
    def log_file_error(
        self,
        error_code: ErrorCode,
        file_path: str,
        exception: Optional[Exception] = None,
        **additional_context
    ) -> None:
        """Log file-related error"""
        context = {'file_path': file_path, **additional_context}
        self.log_error(error_code, exception, **context)

    def log_aws_error(
        self,
        error_code: ErrorCode,
        service: str,
        operation: str,
        exception: Optional[Exception] = None,
        **additional_context
    ) -> None:
        """Log AWS service error"""
        context = {
            'service': service,
            'operation': operation,
            **additional_context
        }
        self.log_error(error_code, exception, **context)

    def log_processing_error(
        self,
        error_code: ErrorCode,
        report_type: str,
        processing_stage: str,
        exception: Optional[Exception] = None,
        **additional_context
    ) -> None:
        """Log data processing error"""
        context = {
            'report_type': report_type,
            'processing_stage': processing_stage,
            **additional_context
        }
        self.log_error(error_code, exception, **context)

    def log_rule_error(
        self,
        error_code: ErrorCode,
        rule_id: str,
        rule_type: str,
        exception: Optional[Exception] = None,
        **additional_context
    ) -> None:
        """Log business rule error"""
        context = {
            'rule_id': rule_id,
            'rule_type': rule_type,
            **additional_context
        }
        self.log_error(error_code, exception, **context)

    def log_batch_error(
        self,
        error_code: ErrorCode,
        operation_type: str,
        file_count: int = None,
        exception: Optional[Exception] = None,
        **additional_context
    ) -> None:
        """Log batch processing error"""
        context = {
            'operation_type': operation_type,
            'file_count': file_count,
            **additional_context
        }
        self.log_error(error_code, exception, **context)

    # Convenience methods for success logging
    def log_file_success(
        self,
        success_code: SuccessCode,
        file_path: str,
        **additional_context
    ) -> None:
        """Log file operation success"""
        context = {'file_path': file_path, **additional_context}
        self.log_success(success_code, **context)

    def log_processing_success(
        self,
        success_code: SuccessCode,
        report_type: str,
        processing_stage: str,
        **additional_context
    ) -> None:
        """Log data processing success"""
        context = {
            'report_type': report_type,
            'processing_stage': processing_stage,
            **additional_context
        }
        self.log_success(success_code, **context)

    def log_batch_success(
        self,
        success_code: SuccessCode,
        operation_type: str,
        file_count: int = None,
        **additional_context
    ) -> None:
        """Log batch processing success"""
        context = {
            'operation_type': operation_type,
            'file_count': file_count,
            **additional_context
        }
        self.log_success(success_code, **context)
