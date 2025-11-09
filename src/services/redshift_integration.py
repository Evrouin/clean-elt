from typing import Dict, Any, List
from src.services.aws.redshift_service import RedshiftService
from src.services.aws.dynamodb_service import DynamoDBService
from src.utils.redshift_config import RedshiftConfig
from src.utils.copy_builder import CopyCommandBuilder
from src.utils.error_logger import ErrorLogger
from src.utils.status_codes import ErrorCode
from src.models.enums import ReportType


class RedshiftIntegration:
    """Service for integrating validated data with Redshift"""
    
    def __init__(self):
        self.logger = ErrorLogger(__name__)
        self.config = RedshiftConfig.get_config()
        self.redshift_service = RedshiftService(**self.config)
        self.dynamodb_service = DynamoDBService()
        self.iam_role_arn = RedshiftConfig.get_iam_role_arn()
    
    def copy_valid_data(
        self,
        s3_path: str,
        report_type: ReportType,
        validation_summary: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Copy valid data from S3 to Redshift after validation"""
        
        try:
            # Only proceed if validation passed
            if validation_summary.get('status') != 'SUCCESS':
                self.logger.log_info(
                    InfoCode.INFO_105,
                    operation="redshift_copy",
                    reason="validation_failed",
                    validation_status=validation_summary.get('status'),
                )
                return {'status': 'SKIPPED', 'reason': 'validation_failed'}
            
            # Build COPY command
            source_file = s3_path.split('/')[-1]
            copy_command = CopyCommandBuilder.build_copy_with_columns(
                report_type=report_type,
                s3_path=s3_path,
                iam_role_arn=self.iam_role_arn,
                source_file=source_file
            )
            
            copy_result = self.redshift_service.execute_copy_command(
                table_name=f"{report_type.display_name}_reports",
                s3_path=s3_path,
                iam_role=self.iam_role_arn,
                copy_options={'ignoreheader': 1}
            )
            
            if copy_result['status'] == 'SUCCESS':
                self._log_data_quality_audit(
                    source_file=source_file,
                    table_name=f"{report_type.display_name}_reports",
                    validation_summary=validation_summary,
                    copy_result=copy_result
                )
            
            return copy_result
            
        except Exception as e:
            self.logger.log_error(
                ErrorCode.RS_305,
                exception=e,
                s3_path=s3_path,
                report_type=report_type.value,
                operation="copy_valid_data",
            )
            return {
                'status': 'FAILED',
                'error': str(e),
                's3_path': s3_path
            }
    
    def _log_data_quality_audit(
        self,
        source_file: str,
        table_name: str,
        validation_summary: Dict[str, Any],
        copy_result: Dict[str, Any]
    ):
        """Log data quality audit record"""
        
        try:
            audit_query = """
            INSERT INTO data_quality_audit 
            (source_file, table_name, total_rows, valid_rows, invalid_rows, validation_errors)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            self.redshift_service.execute_query(audit_query, [
                source_file,
                table_name,
                validation_summary.get('total_rows', 0),
                validation_summary.get('valid_rows', 0),
                validation_summary.get('invalid_rows', 0),
                str(validation_summary.get('errors', []))
            ])
            
            self.logger.log_info(
                InfoCode.INFO_100,
                operation="data_quality_audit_logged",
                source_file=source_file,
                rows_loaded=copy_result.get('rows_loaded', 0),
                table_name=table_name,
            )
            
        except Exception as e:
            self.logger.log_error(
                ErrorCode.RS_306,
                exception=e,
                source_file=source_file,
                table_name=table_name,
                operation="log_audit_record",
            )
    
    def close(self):
        """Close connections"""
        if hasattr(self, 'redshift_service'):
            self.redshift_service.close_connection()
