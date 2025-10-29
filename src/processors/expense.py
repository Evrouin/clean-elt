from typing import Dict, Any
from src.processors.base import BaseProcessor
from src.validators.expense import ExpenseValidator
from src.models.reports.expense import ExpenseReport
from src.utils.file_parser import FileParser
from src.models.enums import ReportType


class ExpenseProcessor(BaseProcessor):
    """Expense report processor with business rules integration"""

    def process(self, bucket: str, key: str) -> Dict[str, Any]:
        file_id = f"{bucket}/{key}"

        try:
            # Read file from S3
            file_data = self.s3_service.get_object_content(bucket, key)

            # Parse file data using utility
            parsed_data = FileParser.parse_file_data(file_data, key)

            validation_results = []
            for row_index, row_data in enumerate(parsed_data):
                # Transform data using model
                model = ExpenseReport(file_name=key.split('/')[-1], bucket=bucket, key=key)
                transformed_data = model.transform_data(row_data)

                # Validate using both field validation and business rules
                validation_result = self.validate_with_business_rules('EXPENSE', transformed_data)
                validation_result['row_index'] = row_index + 1
                validation_result['original_data'] = row_data

                validation_results.append(validation_result)

                # Store invalid results in DynamoDB
                if not validation_result['is_valid']:
                    self.validation_results.store_validation_result(
                        file_id,
                        row_index + 1,
                        validation_result['field_errors'],
                        validation_result['business_violations']
                    )

            # Log processing summary
            total_rows = len(validation_results)
            valid_rows = sum(1 for r in validation_results if r['is_valid'])
            invalid_rows = total_rows - valid_rows

            self.file_processing_log.log_processing(
                file_id, 'completed', total_rows, valid_rows, invalid_rows
            )

            return {
                'report_type': ReportType.EXPENSE.display_name,
                'status': 'processed',
                'total_rows': total_rows,
                'valid_rows': valid_rows,
                'invalid_rows': invalid_rows,
                'file_location': file_id
            }

        except Exception as e:
            # Store error details
            self.error_details.store_error(file_id, 'PROCESSING_ERROR', str(e))

            return {
                'report_type': ReportType.EXPENSE.display_name,
                'status': 'error',
                'error': str(e),
                'file_location': file_id
            }

    def get_validator(self):
        return ExpenseValidator()

    def get_model_class(self):
        return ExpenseReport
