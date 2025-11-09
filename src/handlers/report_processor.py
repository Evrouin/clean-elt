from typing import Dict, Any
from .base import BaseLambdaHandler
from src.models.requests.sqs_request import SQSRequest
from src.models.responses.processing_response import ProcessingResponse
from src.processors.report import ReportProcessor
from src.utils.error_logger import ErrorLogger
from src.utils.status_codes import ErrorCode, SuccessCode, InfoCode


class ReportProcessorHandler(BaseLambdaHandler):
    """Standardized handler for processing report files from SQS events"""

    def __init__(self):
        super().__init__()
        self.processor = ReportProcessor()
        self.error_logger = ErrorLogger(__name__)

    def parse_request(self, event: Dict[str, Any], context: Any) -> SQSRequest:
        """Parse SQS event into structured request model"""
        try:
            self.logger.debug("Parsing SQS event", event=event)
            return SQSRequest.from_sqs_event(event, context)
        except Exception as e:
            self.error_logger.log_error(
                ErrorCode.REQ_601,
                exception=e,
                event_type=type(event).__name__,
            )
            raise ValueError(f"Invalid SQS event format: {str(e)}")

    def process_request(self, request: SQSRequest) -> ProcessingResponse:
        """Process reports"""

        # Log request processing initialization
        self.error_logger.log_info(
            InfoCode.INFO_101,
            operation="request_processing",
            total_records=len(request.records),
            request_id=request.request_id,
            memory_optimized=True,
        )

        results = self.processor.process_reports(request.records)
        recommendations = self.processor.get_processing_recommendations(results)
        response = ProcessingResponse.create_success(results)

        if recommendations.get('recommendations'):
            self.logger.log_info(
                InfoCode.INFO_105,
                operation="processing_recommendations",
                recommendations=recommendations['recommendations'],
            )

        self.logger.log_info(
            InfoCode.INFO_100,
            operation="request_processing_completed",
            total_files=response.total_files,
            successful_files=response.successful_files,
            failed_files=response.failed_files,
            violation_rate=recommendations.get('violation_rate', 0),
            memory_optimized=True,
        )

        # Log processing success
        if response.successful_files > 0:
            self.error_logger.log_success(
                SuccessCode.SUCCESS_200,
                operation="request_processing",
                total_files=response.total_files,
                successful_files=response.successful_files,
            )

        return response

    def handle_error(self, error: Exception, request: SQSRequest = None) -> ProcessingResponse:
        """Error handling"""

        self.error_logger.log_error(
            ErrorCode.REQ_603,
            exception=error,
            request_id=request.request_id if request else None,
        )

        return ProcessingResponse.create_error(str(error))


def process_file(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point with memory monitoring"""
    handler = ReportProcessorHandler()
    return handler.handle(event, context)
