from typing import Dict, Any, List
from src.models.enums import ReportType
from src.utils.schema_validator import SchemaValidator


class CopyCommandBuilder:
    """Builder for Redshift COPY commands"""

    @staticmethod
    def build_copy_command(
        table_name: str,
        s3_path: str,
        iam_role_arn: str,
        options: Dict[str, Any] = None
    ) -> str:
        """Build COPY command for S3 to Redshift"""

        default_options = {
            'delimiter': ',',
            'ignoreheader': 1,
            'removequotes': True,
            'emptyasnull': True,
            'blanksasnull': True,
            'dateformat': 'auto',
            'timeformat': 'auto'
        }

        if options:
            default_options.update(options)

        copy_sql = f"COPY {table_name} FROM '{s3_path}' IAM_ROLE '{iam_role_arn}'"

        for option, value in default_options.items():
            if option == 'format':
                continue
            if isinstance(value, bool) and value:
                copy_sql += f" {option.upper()}"
            elif isinstance(value, (int, float)):
                copy_sql += f" {option.upper()} {value}"
            elif not isinstance(value, bool):
                copy_sql += f" {option.upper()} '{value}'"

        return copy_sql

    @staticmethod
    def build_copy_with_columns(
        report_type: ReportType,
        s3_path: str,
        iam_role_arn: str,
        source_file: str = None
    ) -> str:
        """Build COPY command with specific column mapping"""

        table_name = SchemaValidator.get_table_name(report_type)
        columns = SchemaValidator.get_copy_columns(report_type)

        # Add metadata columns
        if source_file:
            columns_with_metadata = columns + [f"'{source_file}' as source_file"]
        else:
            columns_with_metadata = columns + ["source_file"]

        column_list = f"({', '.join(columns_with_metadata)})"

        copy_sql = CopyCommandBuilder.build_copy_command(
            f"{table_name} {column_list}",
            s3_path,
            iam_role_arn
        )

        return copy_sql
