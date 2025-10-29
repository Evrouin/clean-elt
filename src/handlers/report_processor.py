from typing import Dict, Any
from .base import BaseLambdaHandler
from src.models.requests.sqs_request import SQSRequest
from src.models.responses.processing_response import ProcessingResponse
from src.processors.report import ReportProcessor


class ReportProcessorHandler(BaseLambdaHandler):
    """Standardized handler for processing report files from SQS events"""

    def __init__(self):
        super().__init__()
        self.processor = ReportProcessor()

    def parse_request(self, event: Dict[str, Any], context: Any) -> SQSRequest:
        """Parse SQS event into structured request model"""
        try:
            self.logger.debug("Parsing SQS event", event=event)
            return SQSRequest.from_sqs_event(event, context)
        except Exception as e:
            self.logger.error("Failed to parse SQS request", error=str(e))
            raise ValueError(f"Invalid SQS event format: {str(e)}")

    def process_request(self, request: SQSRequest) -> ProcessingResponse:
        """Process reports with optimization"""
        
        self.logger.info(
            "Processing request with optimization",
            total_records=len(request.records),
            request_id=request.request_id,
            memory_optimized=True
        )

        results = self.processor.process_reports(request.records)
        recommendations = self.processor.get_processing_recommendations(results)
        response = ProcessingResponse.create_success(results)

        if recommendations.get('recommendations'):
            self.logger.info(
                "Processing recommendations",
                recommendations=recommendations['recommendations']
        )

        self.logger.info(
            "Request processing completed with optimization",
            total_files=response.total_files,
            successful_files=response.successful_files,
            failed_files=response.failed_files,
            violation_rate=recommendations.get('violation_rate', 0),
            memory_optimized=True
        )

        return response

    def handle_error(self, error: Exception, request: SQSRequest = None) -> ProcessingResponse:
        """Enhanced error handling"""
        
        self.logger.error(
            "Handler error occurred",
            error=str(error),
            request_id=request.request_id if request else None,
            exc_info=True
        )

        return ProcessingResponse.create_error(str(error))


def process_file(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Optimized Lambda entry point with memory monitoring"""
    handler = ReportProcessorHandler()
    return handler.handle(event, context)
