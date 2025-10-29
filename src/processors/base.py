from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator, Optional
import gc
from functools import lru_cache
from src.validators.business_rules import BusinessRulesValidator
from src.services.aws.s3_service import S3Service
from src.utils.file_processing import FileProcessingUtils
from src.utils.logger import StructuredLogger


class BaseProcessor(ABC):
    """Memory-optimized base processor with streaming and caching"""

    def __init__(self):
        self.logger = StructuredLogger(__name__)
        self.s3_service = S3Service()
        self.file_utils = FileProcessingUtils()
        self._business_rules = None
        self._validator_cache = {}

    @property
    def business_rules(self) -> BusinessRulesValidator:
        """Lazy-loaded business rules validator"""
        if self._business_rules is None:
            self._business_rules = BusinessRulesValidator()
        return self._business_rules

    @lru_cache(maxsize=32)
    def get_cached_validator(self, report_type: str):
        """Cache validator instances per report type"""
        if report_type not in self._validator_cache:
            self._validator_cache[report_type] = self.get_validator()
        return self._validator_cache[report_type]

    def validate_with_business_rules(self, report_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Optimized validation with minimal memory footprint"""

        validator = self.get_cached_validator(report_type)

        field_errors = validator.validate(data)

        # Convert string errors to dict format if needed
        if field_errors and isinstance(field_errors[0], str):
            field_errors = [{'field': 'validation', 'error': error, 'severity': 'ERROR'} for error in field_errors]

        business_violations = []
        if not field_errors or not any(e.get('severity') == 'CRITICAL' for e in field_errors):
            business_violations = self.business_rules.execute_rules(report_type, data)

        return {
            'field_errors': field_errors,
            'business_violations': business_violations,
            'is_valid': len(field_errors) == 0 and len(business_violations) == 0
        }

    @abstractmethod
    def process(self, bucket: str, key: str) -> Dict[str, Any]:
        """Process the report file"""
        pass

    @abstractmethod
    def get_validator(self):
        """Return validator instance"""
        pass

    @abstractmethod
    def get_model_class(self):
        """Return the appropriate model class for this processor"""
        pass

    @abstractmethod
    def _quick_validation_check(self, data: Dict[str, Any]) -> bool:
        """Quick validation check for essential fields"""
        pass

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for monitoring"""
        base_metrics = {
            'processor_type': self.__class__.__name__,
            'cache_size': len(self._validator_cache),
            'memory_optimized': True
        }

        if hasattr(self.business_rules, 'get_performance_stats'):
            base_metrics.update(self.business_rules.get_performance_stats())

        return base_metrics

    def cleanup(self):
        """Cleanup resources and clear caches"""
        self._validator_cache.clear()
        if self._business_rules:
            self._business_rules.clear_cache()
        gc.collect()
