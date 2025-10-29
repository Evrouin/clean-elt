from typing import Dict, Any
from src.processors.base import BaseProcessor
from src.validators.expense import ExpenseValidator
from src.models.reports.expense import ExpenseReport
from src.utils.file_parser import FileParser
from src.models.enums import ReportType


class ExpenseProcessor(BaseProcessor):
    """Expense report processor with business rules integration"""
    
    def get_validator(self):
        return ExpenseValidator()

    def get_model_class(self):
        return ExpenseReport

    def process(self, bucket: str, key: str) -> Dict[str, Any]:
        """Optimized processing with memory management and performance tracking"""
        file_id = f"{bucket}/{key}"

        memory_estimate = self.s3_service.estimate_processing_memory(bucket, key)
        self.logger.info("Processing estimates", **memory_estimate)

        batch_size = self._calculate_optimal_batch_size(memory_estimate['file_size_bytes'])

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
            file_data = self.s3_service.get_object_content(bucket, key)

            parsed_data = FileParser.parse_file_data(file_data, key)

            validation_results = []
            for row_index, row_data in enumerate(parsed_data):
                if not isinstance(row_data, dict):
                    continue

                model = ExpenseReport(file_name=key.split('/')[-1], bucket=bucket, key=key)
                transformed_data = model.transform_data(row_data)

                validation_result = self.validate_with_business_rules('EXPENSE', transformed_data)
                validation_result['row_index'] = row_index + 1

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
            self.logger.error(f"Expense processing failed: {e}")
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
        required_fields = ['expense_id', 'amount', 'category', 'employee_id']
        return all(field in data and data[field] is not None for field in required_fields)

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for monitoring"""
        base_metrics = {
            'processor_type': 'OptimizedExpenseProcessor',
            'cache_size': len(self._validator_cache),
            'memory_optimized': True
        }

        if hasattr(self.business_rules, 'get_performance_stats'):
            base_metrics.update(self.business_rules.get_performance_stats())

        return base_metrics
