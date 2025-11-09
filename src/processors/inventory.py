from typing import Dict, Any
from src.processors.base import BaseProcessor
from src.validators.inventory import InventoryValidator
from src.models.reports.inventory import InventoryReport
from src.utils.memory_profiler import calculate_optimal_batch_size
from src.models.enums import ReportType


class InventoryProcessor(BaseProcessor):
    """Inventory report processor with business rules integration"""

    def get_validator(self):
        return InventoryValidator()

    def get_model_class(self):
        return InventoryReport

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

                    # Ensure row_data is a dictionary
                    if not isinstance(row_data, dict):
                        from src.utils.error_logger import ErrorLogger
                        from src.utils.status_codes import WarningCode

                        error_logger = ErrorLogger(__name__)
                        error_logger.log_warning(
                            WarningCode.FILE_W102,
                            row_index=row_index,
                            row_type=type(row_data).__name__,
                            expected_type="dict",
                        )
                        continue

                    # Transform data using model
                    model = InventoryReport(file_name=key.split('/')[-1], bucket=bucket, key=key)
                    transformed_data = model.transform_data(row_data)

                    # Validate using business rules
                    validation_result = self.validate_with_business_rules('INVENTORY', transformed_data)
                    validation_result['row_index'] = row_index

                    # Update processing stats
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
                ErrorCode.DATA_402,
                report_type="inventory",
                processing_stage="main_processing",
                exception=e,
                bucket=bucket,
                key=key,
                rows_processed=processing_stats['total_rows'],
            )
            processing_stats['status'] = 'FAILED'
            processing_stats['error'] = str(e)
            return processing_stats

    def _calculate_optimal_batch_size(self, file_size_bytes: int) -> int:
        """Calculate optimal batch size based on file size and available memory"""
        if file_size_bytes < 1024 * 1024:  # < 1MB
            return 50
        elif file_size_bytes < 10 * 1024 * 1024:  # < 10MB
            return 100
        elif file_size_bytes < 100 * 1024 * 1024:  # < 100MB
            return 200
        else:  # > 100MB
            return 500

    def _quick_validation_check(self, data: Dict[str, Any]) -> bool:
        """Quick validation check for essential fields"""
        required_fields = ['product_id', 'quantity_on_hand', 'reorder_level']
        return all(field in data and data[field] is not None for field in required_fields)
