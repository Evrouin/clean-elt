from typing import Any, Dict, List

from src.models.reports.factory import ReportFactory
from src.models.requests.sqs_request import S3EventRecord
from src.models.responses.processing_response import FileProcessingResult
from src.utils.batch_copy import BatchCopyManager
from src.utils.error_logger import ErrorLogger
from src.utils.status_codes import ErrorCode, WarningCode, SuccessCode, InfoCode


class ReportProcessor:
    """Main report processor that coordinates file processing"""

    def __init__(self):
        self.logger = ErrorLogger(__name__)
        self.report_factory = ReportFactory()
        self.batch_copy_manager = BatchCopyManager()

    def process_reports(self, s3_records: List[S3EventRecord]) -> List[FileProcessingResult]:
        """Process multiple S3 records"""
        results = []

        for record in s3_records:
            # Log processing initialization
            self.logger.log_info(
                InfoCode.INFO_101,
                operation="file_processing",
                bucket=record.bucket,
                key=record.key,
            )
            
            # Log file processing start
            self.logger.log_info(
                InfoCode.INFO_102,
                operation="s3_file_processing",
                bucket=record.bucket,
                key=record.key,
                size=record.size,
            )

            try:
                processor = self.report_factory.create_processor(record.key)
                result = processor.process(record.bucket, record.key)

                # Log processing success
                self.logger.log_processing_success(
                    SuccessCode.SUCCESS_200,
                    report_type=record.key.split('/')[1] if '/' in record.key else 'unknown',
                    processing_stage="file_processing",
                    total_rows=result.get('total_rows', 0),
                    valid_rows=result.get('valid_rows', 0),
                )

                redshift_result = None
                valid_rows = result.get('valid_rows', 0)
                
                # Log integration check
                self.logger.log_info(
                    InfoCode.INFO_102,
                    operation="redshift_integration_check",
                    valid_rows=valid_rows,
                    result_keys=list(result.keys()),
                    result_status=result.get('status'),
                )

                if valid_rows > 0:
                    self.logger.log_info(
                        InfoCode.INFO_104,
                        operation="redshift_integration_triggered",
                        valid_rows=valid_rows,
                    )
                    try:
                        from src.models.enums import ReportType
                        from src.services.redshift_integration import RedshiftIntegration

                        try:
                            report_type = ReportType.from_s3_key(record.key)
                            self.logger.log_info(
                                InfoCode.INFO_100,
                                operation="report_type_detection",
                                report_type=report_type.name,
                                file_key=record.key,
                            )

                            redshift_integration = RedshiftIntegration()
                            redshift_result = redshift_integration.copy_valid_data(
                                s3_path=f"s3://{record.bucket}/{record.key}",
                                report_type=report_type,
                                validation_summary=result
                            )
                            redshift_integration.close()

                            self.logger.log_info(
                                InfoCode.INFO_100,
                                operation="redshift_integration_completed",
                                redshift_status=redshift_result.get('status'),
                                rows_loaded=redshift_result.get('rows_loaded', 0),
                            )
                            
                            # Log integration success
                            if redshift_result.get('status') == 'SUCCESS':
                                self.logger.log_success(
                                    SuccessCode.SUCCESS_200,
                                    operation="redshift_integration",
                                    rows_loaded=redshift_result.get('rows_loaded', 0),
                                )
                        except ValueError as ve:
                            self.logger.log_warning(
                                WarningCode.DATA_W401,
                                file_key=record.key,
                                error_message=str(ve),
                            )

                    except Exception as e:
                        self.logger.log_error(
                            ErrorCode.RS_305,
                            exception=e,
                            bucket=record.bucket,
                            key=record.key,
                            valid_rows=valid_rows,
                        )
                        redshift_result = {'status': 'FAILED', 'error': str(e)}
                else:
                    # Log skipping integration
                    self.logger.log_info(
                        InfoCode.INFO_105,
                        operation="redshift_integration",
                        reason="no_valid_rows",
                        valid_rows=valid_rows,
                    )

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
                    self.logger.log_info(
                        InfoCode.INFO_100,
                        operation="performance_metrics",
                        **metrics,
                    )

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

        # Log batch processing completion
        self.logger.log_info(
            InfoCode.INFO_100,
            operation="batch_processing_completed",
            files_processed=len(s3_records),
        )
        
        # Log batch processing success
        successful_files = len([r for r in results if r.status == 'SUCCESS'])
        if successful_files > 0:
            self.logger.log_batch_success(
                SuccessCode.SUCCESS_200,
                operation_type="batch_processing",
                file_count=len(s3_records),
                successful_files=successful_files,
            )

        # Execute batch COPY if multiple valid files
        if len(results) > 1:
            # Log batch processing initialization
            self.logger.log_info(
                InfoCode.INFO_101,
                operation="batch_processing",
                file_count=len(results),
            )
            self._execute_batch_copy(s3_records, results)

        return results

    def _execute_batch_copy(self, s3_records: List[S3EventRecord], results: List[FileProcessingResult]):
        """Execute batch COPY operations for validated files"""

        try:
            # Prepare batch data for files with valid rows
            file_batches = []
            for record, result in zip(s3_records, results):
                if result.valid_rows > 0:
                    from src.models.enums import ReportType
                    try:
                        report_type = ReportType.from_s3_key(record.key)
                        file_batches.append({
                            's3_path': f"s3://{record.bucket}/{record.key}",
                            'report_type': report_type,
                            'validation_summary': {
                                'status': 'SUCCESS',
                                'total_rows': result.total_rows,
                                'valid_rows': result.valid_rows,
                                'invalid_rows': result.invalid_rows
                            }
                        })
                    except ValueError:
                        self.logger.log_warning(
                            WarningCode.DATA_W402,
                            file_key=record.key,
                            operation="batch_processing",
                        )

            if len(file_batches) > 1:
                self.logger.log_info(
                    InfoCode.INFO_102,
                    operation="batch_copy_execution",
                    file_count=len(file_batches),
                )

                # Use manifest-based batch copy for multiple files
                manifest_bucket = s3_records[0].bucket  # Use same bucket for manifest
                batch_results = self.batch_copy_manager.batch_copy_with_manifest(
                    file_batches=file_batches,
                    manifest_bucket=manifest_bucket,
                    report_type=file_batches[0]['report_type']  # Group by first type
                )

                self.logger.log_info(
                    InfoCode.INFO_100,
                    operation="batch_copy_completed",
                    status=batch_results.get('status'),
                    total_files=batch_results.get('total_files'),
                )
                
                # Log batch COPY success
                if batch_results.get('status') == 'SUCCESS':
                    self.logger.log_batch_success(
                        SuccessCode.SUCCESS_200,
                        operation_type="batch_copy",
                        file_count=batch_results.get('total_files'),
                    )
            else:
                # Log skipping batch COPY
                self.logger.log_info(
                    InfoCode.INFO_105,
                    operation="batch_copy",
                    reason="insufficient_files",
                    file_count=len(file_batches),
                )

        except Exception as e:
            self.logger.log_batch_error(
                ErrorCode.BATCH_703,
                operation_type="batch_execution",
                exception=e,
                file_count=len(s3_records),
            )

    def process_single_file(self, bucket: str, key: str) -> FileProcessingResult:
        """Process a single file"""

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
            self.logger.log_processing_error(
                ErrorCode.DATA_404,
                report_type="unknown",
                processing_stage="single_file",
                exception=e,
                bucket=bucket,
                key=key,
            )

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

    def process_batch_with_manifest(self, s3_paths: List[str], manifest_bucket: str) -> Dict[str, Any]:
        """Process multiple files using manifest-based batch COPY"""

        try:
            from src.models.enums import ReportType

            # Group files by report type
            grouped_files = {}
            for s3_path in s3_paths:
                key = s3_path.split('/')[-1]
                try:
                    report_type = ReportType.from_s3_key(key)
                    if report_type not in grouped_files:
                        grouped_files[report_type] = []
                    grouped_files[report_type].append(s3_path)
                except ValueError:
                    self.logger.log_warning(
                        WarningCode.DATA_W402,
                        file_key=key,
                        operation="manifest_processing",
                    )

            results = {}
            for report_type, paths in grouped_files.items():
                if len(paths) > 1:
                    # Use manifest for multiple files
                    result = self.batch_copy_manager.create_manifest_copy(
                        s3_paths=paths,
                        manifest_s3_path=f"s3://{manifest_bucket}/manifests/",
                        report_type=report_type
                    )
                    results[report_type.value] = result
                else:
                    self.logger.log_info(
                        InfoCode.INFO_100,
                        operation="single_file_processing",
                        report_type=report_type.value,
                        reason="single_file_only",
                    )

            return {
                'status': 'SUCCESS',
                'batch_results': results,
                'total_groups': len(grouped_files)
            }

        except Exception as e:
            self.logger.log_batch_error(
                ErrorCode.BATCH_704,
                operation_type="manifest_processing",
                exception=e,
                file_count=len(s3_paths),
            )
            return {
                'status': 'FAILED',
                'error': str(e)
            }
