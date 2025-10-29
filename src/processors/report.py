from typing import List
from src.models.requests.sqs_request import S3EventRecord
from src.models.responses.processing_response import FileProcessingResult
from src.models.reports.factory import ReportFactory
from src.utils.logger import Logger


class ReportProcessor:
    """Main report processor that coordinates file processing"""

    def __init__(self):
        self.logger = Logger(self.__class__.__name__)
        self.report_factory = ReportFactory()

    def process_reports(self, s3_records: List[S3EventRecord]) -> List[FileProcessingResult]:
        """Process multiple S3 records and return results"""
        results = []

        for record in s3_records:
            self.logger.info(
                "Processing S3 file",
                bucket=record.bucket,
                key=record.key,
                size=record.size,
            )

            try:
                # Use factory to create appropriate processor
                processor = self.report_factory.create_processor(record.key)
                result = processor.process(record.bucket, record.key)

                # Convert to standardized result model
                file_result = FileProcessingResult(
                    report_type=result.get('report_type', 'unknown'),
                    status=result.get('status', 'unknown'),
                    total_rows=result.get('total_rows', 0),
                    valid_rows=result.get('valid_rows', 0),
                    invalid_rows=result.get('invalid_rows', 0),
                    file_location=result.get('file_location', f"{record.bucket}/{record.key}"),
                    error=result.get('error')
                )

                results.append(file_result)

                self.logger.info(
                    "File processing completed",
                    report_type=file_result.report_type,
                    status=file_result.status,
                    total_rows=file_result.total_rows,
                    valid_rows=file_result.valid_rows,
                    invalid_rows=file_result.invalid_rows,
                )

            except ValueError as e:
                # Handle factory errors (unknown report type)
                error_result = FileProcessingResult(
                    report_type='unknown',
                    status='error',
                    file_location=f"{record.bucket}/{record.key}",
                    error=str(e)
                )
                results.append(error_result)
                self.logger.error("Processing error", error=str(e), key=record.key)

            except Exception as e:
                # Handle unexpected errors
                error_result = FileProcessingResult(
                    report_type='unknown',
                    status='error',
                    file_location=f"{record.bucket}/{record.key}",
                    error=f"Unexpected error: {str(e)}"
                )
                results.append(error_result)
                self.logger.error("Unexpected processing error", error=str(e), key=record.key)

        return results
