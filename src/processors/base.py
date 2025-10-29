from abc import ABC, abstractmethod
from typing import Dict, Any
from ..services.business_rules_service import BusinessRulesService
from ..services.aws.s3_service import S3Service
from src.models.dynamodb.validation_results import ValidationResults
from src.models.dynamodb.file_processing_log import FileProcessingLog
from src.models.dynamodb.error_details import ErrorDetails


class BaseProcessor(ABC):
    """Abstract base class for all report processors"""

    def __init__(self):
        self.s3_service = S3Service()
        self.business_rules = BusinessRulesService()

        # DynamoDB models
        self.validation_results = ValidationResults()
        self.file_processing_log = FileProcessingLog()
        self.error_details = ErrorDetails()

    def validate_with_business_rules(self, report_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data using both field validation and business rules"""

        # Step 1: Field validation using Pydantic validator
        validator = self.get_validator()
        field_errors = validator.validate(data)

        # Step 2: Business rules validation from DynamoDB
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
        """Return the appropriate validator for this processor"""
        pass

    @abstractmethod
    def get_model_class(self):
        """Return the appropriate model class for this processor"""
        pass
