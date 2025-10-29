import logging
import json
import os
from typing import Any, Dict


class Logger:
    """Custom logger for Lambda functions"""

    def __init__(self, name: str = __name__):
        self.logger = logging.getLogger(name)

        # Set log level from environment variable or default to INFO
        log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()
        self.logger.setLevel(getattr(logging, log_level, logging.INFO))

        # Only add handler if none exists (prevents duplicate logs)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '[%(levelname)s] %(asctime)s - %(name)s - %(message)s'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, message: str, **kwargs):
        """Log info message with optional context"""
        self._log_with_context(self.logger.info, message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message with optional context"""
        self._log_with_context(self.logger.error, message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message with optional context"""
        self._log_with_context(self.logger.warning, message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message with optional context"""
        self._log_with_context(self.logger.debug, message, **kwargs)

    def log_event(self, event: Dict[str, Any], context: Any = None):
        """Log Lambda event details"""
        event_info = {
            'event_source': event.get('Records', [{}])[0].get('eventSource', 'unknown'),
            'records_count': len(event.get('Records', [])),
            'request_id': getattr(context, 'aws_request_id', 'unknown') if context else 'unknown'
        }

        self.info("Lambda invocation started", **event_info)
        self.debug("Full event", event=self._sanitize_event(event))

    def _log_with_context(self, log_func, message: str, **kwargs):
        """Log message with additional context"""
        if kwargs:
            context_str = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            log_func(f"{message} | {context_str}")
        else:
            log_func(message)

    def _sanitize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Remove sensitive data from event for logging"""
        # Create a copy to avoid modifying original event
        sanitized = json.loads(json.dumps(event, default=str))

        # Remove or mask sensitive fields if needed
        # For now, just return as-is since it's S3/SQS events
        return sanitized
