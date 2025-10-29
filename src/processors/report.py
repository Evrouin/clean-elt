from typing import List, Dict, Any
from src.models.requests.sqs_request import S3EventRecord
from src.models.responses.processing_response import FileProcessingResult
from src.models.reports.factory import ReportFactory
from src.utils.logger import StructuredLogger


class ReportProcessor:
    """Main report processor that coordinates file processing"""

    def __init__(self):
        self.logger = StructuredLogger(__name__)
        self.report_factory = ReportFactory()

    def process_reports(self, s3_records: List[S3EventRecord]) -> List[FileProcessingResult]:
        """Process multiple S3 records with optimization"""
        results = []

        for record in s3_records:
            self.logger.info(
                "Processing S3 file",
                bucket=record.bucket,
                key=record.key,
                size=record.size,
            )

            try:
                processor = self.report_factory.create_processor(record.key)
                result = processor.process(record.bucket, record.key)

                processing_result = FileProcessingResult(
                    report_type=record.key.split('/')[1].upper(),
                    file_location=f"s3://{record.bucket}/{record.key}",
                    status=result.get('status', 'SUCCESS'),
                    total_rows=result.get('total_rows', 0),
                    valid_rows=result.get('valid_rows', 0),
                    invalid_rows=result.get('invalid_rows', 0),
                    error=None if result.get('status') == 'SUCCESS' else result.get('error')
                )

                results.append(processing_result)

                if hasattr(processor, 'get_performance_metrics'):
                    metrics = processor.get_performance_metrics()
                    self.logger.info("Processor performance metrics", **metrics)

                if hasattr(processor, 'cleanup'):
                    processor.cleanup()

            except Exception as e:
                self.logger.error(
                    "Failed to process S3 file",
                    bucket=record.bucket,
                    key=record.key,
                    error=str(e),
                    exc_info=True
                )

                failed_result = FileProcessingResult(
                    report_type=record.key.split('/')[1].upper() if '/' in record.key else 'UNKNOWN',
                    file_location=f"s3://{record.bucket}/{record.key}",
                    status='FAILED',
                    total_rows=0,
                    valid_rows=0,
                    invalid_rows=0,
                    error=str(e)
                )
                results.append(failed_result)

        self.logger.info(
            "Batch processing completed",
            files_processed=len(s3_records)
        )

        return results

    def process_single_file(self, bucket: str, key: str) -> FileProcessingResult:
        """Process a single file with optimization"""

        try:
            processor = self.report_factory.create_processor(key)
            result = processor.process(bucket, key)

            processing_result = FileProcessingResult(
                report_type=key.split('/')[1].upper() if '/' in key else 'UNKNOWN',
                file_location=f"s3://{bucket}/{key}",
                status=result.get('status', 'SUCCESS'),
                total_rows=result.get('total_rows', 0),
                valid_rows=result.get('valid_rows', 0),
                invalid_rows=result.get('invalid_rows', 0),
                error=None if result.get('status') == 'SUCCESS' else result.get('error')
            )

            if hasattr(processor, 'cleanup'):
                processor.cleanup()

            return processing_result

        except Exception as e:
            self.logger.error(f"Single file processing failed: {e}")

            return FileProcessingResult(
                report_type=key.split('/')[1].upper() if '/' in key else 'UNKNOWN',
                file_location=f"s3://{bucket}/{key}",
                status='FAILED',
                total_rows=0,
                valid_rows=0,
                invalid_rows=0,
                error=str(e)
            )

    def get_processing_recommendations(self, results: List[FileProcessingResult]) -> Dict[str, Any]:
        """Get processing recommendations based on results"""
        total_files = len(results)
        successful_files = len([r for r in results if r.error is None])
        failed_files = total_files - successful_files

        total_rows = sum(r.total_rows for r in results)
        total_invalid_rows = sum(r.invalid_rows for r in results)

        recommendations = []

        if failed_files > 0:
            recommendations.append(f"Review {failed_files} failed file(s) for processing errors")

        if total_rows > 0 and total_invalid_rows > total_rows * 0.1:
            recommendations.append("High invalid row rate detected - review data quality")

        return {
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': failed_files,
            'total_rows_processed': total_rows,
            'total_invalid_rows': total_invalid_rows,
            'invalid_row_rate': total_invalid_rows / max(total_rows, 1),
            'recommendations': recommendations,
            'performance_optimized': True
        }
