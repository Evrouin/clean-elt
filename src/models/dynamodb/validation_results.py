from typing import List, Dict, Any
from datetime import datetime, timedelta
from src.services.aws.dynamodb_service import DynamoDBService


class ValidationResults:
    """Model for validation-results DynamoDB table"""

    TABLE_NAME = 'validation-results'

    def __init__(self, dynamodb_service: DynamoDBService = None):
        self.db = dynamodb_service or DynamoDBService()

    def store_validation_result(self, file_id: str, row_index: int, field_errors: List[str], business_violations: List[Dict]):
        """Store validation result for invalid data"""
        item = {
            'file_id': file_id,
            'timestamp': datetime.now().isoformat(),
            'row_index': row_index,
            'field_errors': field_errors,
            'business_violations': business_violations,
            'ttl': int((datetime.now() + timedelta(days=30)).timestamp())
        }

        self.db.put_item(self.TABLE_NAME, item)

    def get_validation_results(self, file_id: str) -> List[Dict[str, Any]]:
        """Get all validation results for a file"""
        from boto3.dynamodb.conditions import Key

        return self.db.query(
            self.TABLE_NAME,
            Key('file_id').eq(file_id)
        )

    def get_validation_result(self, file_id: str, timestamp: str) -> Dict[str, Any]:
        """Get a specific validation result"""
        return self.db.get_item(
            self.TABLE_NAME,
            {'file_id': file_id, 'timestamp': timestamp}
        )

    def get_results_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get validation results within a date range"""
        return self.db.scan(
            self.TABLE_NAME,
            FilterExpression='#ts BETWEEN :start_date AND :end_date',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={
                ':start_date': start_date,
                ':end_date': end_date
            }
        )
