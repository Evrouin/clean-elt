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
        """Process reports using report processor"""
        self.logger.info(
            "Processing request",
            total_records=len(request.records),
            request_id=request.request_id,
        )

        results = self.processor.process_reports(request.records)

        response = ProcessingResponse.create_success(results)

        self.logger.info(
            "Request processing completed",
            total_files=response.total_files,
            successful_files=response.successful_files,
            failed_files=response.failed_files,
        )

        return response


def process_file(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entry point - creates handler and processes request"""
    handler = ReportProcessorHandler()
    return handler.handle(event, context)
