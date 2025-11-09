from abc import ABC, abstractmethod
from typing import Dict, Any
import json
from src.utils.logger import Logger


class BaseLambdaHandler(ABC):
    """Abstract base class for all Lambda handlers"""

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)

    def handle(self, event: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Main entry point - standardized flow"""
        try:
            self.logger.log_event(event, context)

            # Parse and validate input
            request = self.parse_request(event, context)

            # Process business logic
            response = self.process_request(request)

            # Format response
            return self.format_response(response)

        except Exception as e:
            return self.handle_error(e)

    @abstractmethod
    def parse_request(self, event: Dict[str, Any], context: Any) -> Any:
        """Parse Lambda event into request model"""
        pass

    @abstractmethod
    def process_request(self, request: Any) -> Any:
        """Process business logic"""
        pass

    def format_response(self, response: Any) -> Dict[str, Any]:
        """Format standardized response"""
        return {
            'statusCode': 200,
            'body': json.dumps(response, default=str)
        }

    def handle_error(self, error: Exception) -> Dict[str, Any]:
        """Standardized error handling"""
        from src.utils.error_logger import ErrorLogger
        from src.utils.status_codes import ErrorCode
        
        error_logger = ErrorLogger(__name__)
        error_logger.log_error(
            ErrorCode.REQ_603,
            exception=error,
            error_type=type(error).__name__,
        )
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(error),
                'error_type': type(error).__name__
            })
        }
