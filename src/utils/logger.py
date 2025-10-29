import logging
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional
import traceback


class StructuredLogger:
    def __init__(self, name: str = __name__, correlation_id: Optional[str] = None):
        self.name = name
        self.correlation_id = correlation_id
        self._logger = logging.getLogger(name)

        log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
        self._logger.setLevel(getattr(logging, log_level, logging.INFO))

        if not self._logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(self._get_formatter())
            self._logger.addHandler(handler)

        self._logger.propagate = False

    def _get_formatter(self):
        """Get appropriate formatter based on environment"""
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            return JsonFormatter()
        else:
            return logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

    def _create_log_record(self, level: str, message: str, **kwargs) -> Dict[str, Any]:
        """Create structured log record"""
        record = {
            'timestamp': datetime.now(timezone.utc).isoformat() + 'Z',
            'level': level,
            'logger': self.name,
            'message': message,
        }

        if self.correlation_id:
            record['correlation_id'] = self.correlation_id

        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            record['aws'] = {
                'function_name': os.environ.get('AWS_LAMBDA_FUNCTION_NAME'),
                'function_version': os.environ.get('AWS_LAMBDA_FUNCTION_VERSION'),
                'request_id': os.environ.get('AWS_REQUEST_ID'),
                'region': os.environ.get('AWS_REGION')
            }

        if kwargs:
            record['fields'] = kwargs

        return record

    def info(self, message: str, **kwargs):
        """Log info message with structured data"""
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            record = self._create_log_record('INFO', message, **kwargs)
            self._logger.info(json.dumps(record, default=str, separators=(',', ':')))
        else:
            context = f" {json.dumps(kwargs, default=str)}" if kwargs else ""
            self._logger.info(f"{message}{context}")

    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        """Log error message with optional exception details"""
        if error:
            kwargs.update({
                'error_type': type(error).__name__,
                'error_message': str(error),
                'traceback': traceback.format_exc() if error else None
            })

        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            record = self._create_log_record('ERROR', message, **kwargs)
            self._logger.error(json.dumps(record, default=str, separators=(',', ':')))
        else:
            context = f" {json.dumps(kwargs, default=str)}" if kwargs else ""
            self._logger.error(f"{message}{context}")

    def warning(self, message: str, **kwargs):
        """Log warning message with structured data"""
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            record = self._create_log_record('WARNING', message, **kwargs)
            self._logger.warning(json.dumps(record, default=str, separators=(',', ':')))
        else:
            context = f" {json.dumps(kwargs, default=str)}" if kwargs else ""
            self._logger.warning(f"{message}{context}")

    def debug(self, message: str, **kwargs):
        """Log debug message with structured data"""
        if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
            record = self._create_log_record('DEBUG', message, **kwargs)
            self._logger.debug(json.dumps(record, default=str, separators=(',', ':')))
        else:
            context = f" {json.dumps(kwargs, default=str)}" if kwargs else ""
            self._logger.debug(f"{message}{context}")

    def set_correlation_id(self, correlation_id: str):
        """Set correlation ID for request tracing"""
        self.correlation_id = correlation_id

    def log_event(self, event: Dict[str, Any], context: Any = None):
        """Log Lambda event details for backward compatibility"""
        event_info = {
            'event_source': event.get('Records', [{}])[0].get('eventSource', 'unknown'),
            'records_count': len(event.get('Records', [])),
            'request_id': getattr(context, 'aws_request_id', 'unknown') if context else 'unknown'
        }

        if context and hasattr(context, 'aws_request_id'):
            self.set_correlation_id(context.aws_request_id)

        self.info("Lambda invocation started", **event_info)
        self.debug("Full Lambda event", event=self._sanitize_event(event))

    def _sanitize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive data from event for logging"""
        import json
        sanitized = json.loads(json.dumps(event, default=str))

        return sanitized

    def with_context(self, **context) -> 'ContextLogger':
        """Create a context logger with persistent fields"""
        return ContextLogger(self, context)


class ContextLogger:
    """Logger wrapper that adds persistent context to all log messages"""

    def __init__(self, logger: StructuredLogger, context: Dict[str, Any]):
        self.logger = logger
        self.context = context

    def info(self, message: str, **kwargs):
        self.logger.info(message, **{**self.context, **kwargs})

    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        self.logger.error(message, error=error, **{**self.context, **kwargs})

    def warning(self, message: str, **kwargs):
        self.logger.warning(message, **{**self.context, **kwargs})

    def debug(self, message: str, **kwargs):
        self.logger.debug(message, **{**self.context, **kwargs})


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging in Lambda"""

    def format(self, record):
        return record.getMessage()


def Logger(name: str = __name__) -> StructuredLogger:
    """Create a structured logger instance"""
    return StructuredLogger(name)
