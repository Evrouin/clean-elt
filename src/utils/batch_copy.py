import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

import boto3

from src.models.enums import ReportType
from src.services.aws.redshift_service import RedshiftService
from src.services.redshift_integration import RedshiftIntegration
from src.utils.error_logger import ErrorLogger
from src.utils.status_codes import ErrorCode, SuccessCode, InfoCode


class BatchCopyManager:
    """Manager for batch COPY operations to Redshift"""

    def __init__(self, max_workers: int = 3):
        self.logger = ErrorLogger(__name__)
        self.max_workers = max_workers
        self.s3_client = boto3.client('s3')

    def batch_copy_files(
        self,
        file_batches: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Execute batch COPY operations for multiple files"""

        # Log batch operation initialization
        self.logger.log_info(
            InfoCode.INFO_101,
            operation="batch_copy_files",
            file_count=len(file_batches),
        )

        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._copy_single_file, batch): batch
                for batch in file_batches
            }

            for future in as_completed(future_to_file):
                batch = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    self.logger.log_info(
                        InfoCode.INFO_103,
                        operation="batch_copy_completed",
                        file=batch.get('s3_path'),
                        status=result.get('status'),
                    )
                    
                    # Log batch success
                    if result.get('status') == 'SUCCESS':
                        self.logger.log_success(
                            SuccessCode.SUCCESS_200,
                            operation="batch_copy",
                            file_path=batch.get('s3_path'),
                        )
                except Exception as e:
                    self.logger.log_batch_error(
                        ErrorCode.BATCH_701,
                        operation_type="batch_copy",
                        exception=e,
                        file_path=batch.get('s3_path'),
                    )
                    results.append({
                        'status': 'FAILED',
                        'error': str(e),
                        's3_path': batch.get('s3_path')
                    })

        return results

    def _copy_single_file(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        """Copy single file to Redshift"""

        redshift_integration = RedshiftIntegration()

        try:
            result = redshift_integration.copy_valid_data(
                s3_path=batch['s3_path'],
                report_type=batch['report_type'],
                validation_summary=batch['validation_summary']
            )
            return result
        finally:
            redshift_integration.close()

    def batch_copy_with_manifest(
        self,
        file_batches: List[Dict[str, Any]],
        manifest_bucket: str,
        report_type: ReportType
    ) -> Dict[str, Any]:
        """Execute batch COPY using manifest file"""

        try:
            grouped_files = self._group_files_by_type(file_batches)

            results = {}

            for report_type, files in grouped_files.items():
                if len(files) > 1:
                    result = self._copy_with_manifest(files, manifest_bucket, report_type)
                else:
                    result = self._copy_single_file_direct(files[0])

                results[report_type.value] = result

            return {
                'status': 'SUCCESS',
                'results': results,
                'total_files': len(file_batches)
            }

        except Exception as e:
            self.logger.log_batch_error(
                ErrorCode.BATCH_702,
                operation_type="manifest_copy",
                exception=e,
                file_count=len(file_batches),
            )
            return {
                'status': 'FAILED',
                'error': str(e)
            }

    def _group_files_by_type(self, file_batches: List[Dict[str, Any]]) -> Dict[ReportType, List[Dict]]:
        """Group files by report type for efficient batch processing"""

        grouped = {}

        for batch in file_batches:
            report_type = batch['report_type']
            if report_type not in grouped:
                grouped[report_type] = []
            grouped[report_type].append(batch)

        return grouped

    def _copy_with_manifest(
        self,
        files: List[Dict[str, Any]],
        manifest_bucket: str,
        report_type: ReportType
    ) -> Dict[str, Any]:
        """Create manifest and execute batch COPY"""

        try:
            manifest_entries = []
            for file_batch in files:
                if file_batch['validation_summary'].get('status') == 'SUCCESS':
                    manifest_entries.append({
                        "url": file_batch['s3_path'],
                        "mandatory": True
                    })

            if not manifest_entries:
                return {
                    'status': 'SKIPPED',
                    'reason': 'no_valid_files',
                    'files_count': len(files)
                }

            manifest_content = {"entries": manifest_entries}

            manifest_key = f"manifests/{report_type.value}/batch_{self.logger.get_timestamp()}.manifest"
            manifest_s3_path = f"s3://{manifest_bucket}/{manifest_key}"

            self.s3_client.put_object(
                Bucket=manifest_bucket,
                Key=manifest_key,
                Body=json.dumps(manifest_content),
                ContentType='application/json'
            )

            redshift_service = RedshiftService()

            try:
                table_name = f"{report_type.display_name}_reports"
                iam_role = self._get_iam_role_arn()

                result = redshift_service.execute_manifest_copy(
                    table_name=table_name,
                    manifest_s3_path=manifest_s3_path,
                    iam_role=iam_role
                )

                self.s3_client.delete_object(Bucket=manifest_bucket, Key=manifest_key)

                return result

            finally:
                redshift_service.close_connection()

        except Exception as e:
            self.logger.log_batch_error(
                ErrorCode.BATCH_705,
                operation_type="manifest_operation",
                exception=e,
                file_count=len(files),
                manifest_path=manifest_s3_path,
            )
            return {
                'status': 'FAILED',
                'error': str(e),
                'files_count': len(files)
            }

    def _copy_single_file_direct(self, file_batch: Dict[str, Any]) -> Dict[str, Any]:
        """Execute direct single file COPY using RedshiftService"""

        redshift_service = RedshiftService()

        try:
            table_name = f"{file_batch['report_type'].display_name}_reports"
            iam_role = self._get_iam_role_arn()

            result = redshift_service.execute_copy_command(
                table_name=table_name,
                s3_path=file_batch['s3_path'],
                iam_role=iam_role,
                copy_options={
                    'COMPUPDATE': 'ON',
                    'STATUPDATE': 'ON'
                }
            )

            return result

        finally:
            redshift_service.close_connection()

    def create_manifest_copy(
        self,
        s3_paths: List[str],
        manifest_s3_path: str,
        report_type: ReportType
    ) -> Dict[str, Any]:
        """Create manifest file and execute COPY for multiple files"""

        file_batches = []
        for s3_path in s3_paths:
            file_batches.append({
                's3_path': s3_path,
                'report_type': report_type,
                'validation_summary': {'status': 'SUCCESS'}
            })

        manifest_bucket = manifest_s3_path.split('/')[2]

        return self.batch_copy_with_manifest(file_batches, manifest_bucket, report_type)

    def _get_iam_role_arn(self) -> str:
        """Get IAM role ARN for COPY operations"""
        from src.utils.redshift_config import RedshiftConfig
        return RedshiftConfig.get_iam_role_arn()
