from typing import Dict, Any
from src.processors.base import BaseProcessor
from src.validators.expense import ExpenseValidator
from src.models.reports.expense import ExpenseReport
from src.utils.memory_profiler import calculate_optimal_batch_size
from src.models.enums import ReportType


class ExpenseProcessor(BaseProcessor):
    """Expense report processor with business rules integration"""

    def get_validator(self):
        return ExpenseValidator()

    def get_model_class(self):
        return ExpenseReport

    def process(self, bucket: str, key: str) -> Dict[str, Any]:
        file_id = f"{bucket}/{key}"

        memory_estimate = self.file_utils.estimate_processing_memory(bucket, key)
        batch_size = calculate_optimal_batch_size(memory_estimate['file_size_bytes'])

        processing_stats = {
            'file_id': file_id,
            'total_rows': 0,
            'valid_rows': 0,
            'invalid_rows': 0,
            'business_rule_violations': 0,
            'batches_processed': 0,
            'memory_optimized': True
        }

        try:
            validation_results = []
            row_index = 0

            for batch in self.file_utils.stream_csv_batches(bucket, key, batch_size):
                processing_stats['batches_processed'] += 1

                for row_data in batch:
                    row_index += 1

                    if not isinstance(row_data, dict):
                        continue

                    model = ExpenseReport(file_name=key.split('/')[-1], bucket=bucket, key=key)
                    transformed_data = model.transform_data(row_data)

                    validation_result = self.validate_with_business_rules('EXPENSE', transformed_data)
                    validation_result['row_index'] = row_index

                    processing_stats['total_rows'] += 1
                    if validation_result['is_valid']:
                        processing_stats['valid_rows'] += 1
                    else:
                        processing_stats['invalid_rows'] += 1
                        if validation_result['business_violations']:
                            processing_stats['business_rule_violations'] += len(validation_result['business_violations'])

                    validation_results.append(validation_result)

            self.cleanup()

            processing_stats['status'] = 'SUCCESS'
            processing_stats['validation_results'] = validation_results
            return processing_stats

        except Exception as e:
            from src.utils.error_logger import ErrorLogger
            from src.utils.status_codes import ErrorCode

            error_logger = ErrorLogger(__name__)
            error_logger.log_processing_error(
                ErrorCode.DATA_403,
                report_type="expense",
                processing_stage="main_processing",
                exception=e,
                bucket=bucket,
                key=key,
                rows_processed=processing_stats['total_rows'],
            )
            processing_stats['status'] = 'FAILED'
            processing_stats['error'] = str(e)
            return processing_stats

    def _quick_validation_check(self, data: Dict[str, Any]) -> bool:
        """Quick validation check for essential fields"""
        required_fields = ['expense_id', 'amount', 'category', 'employee_id']
        return all(field in data and data[field] is not None for field in required_fields)
