from typing import List, Dict, Any
from datetime import datetime, timedelta
from src.services.aws.dynamodb_service import DynamoDBService


class ErrorDetails:
    """Model for error-details DynamoDB table"""

    TABLE_NAME = 'error-details'

    def __init__(self, dynamodb_service: DynamoDBService = None):
        self.db = dynamodb_service or DynamoDBService()

    def store_error(self, file_id: str, error_type: str, error_message: str, **metadata):
        """Store detailed error information"""
        error_id = f"{error_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        item = {
            'file_id': file_id,
            'error_id': error_id,
            'error_type': error_type,
            'error_message': error_message,
            'timestamp': datetime.now().isoformat(),
            'ttl': int((datetime.now() + timedelta(days=30)).timestamp()),
            **metadata
        }

        self.db.put_item(self.TABLE_NAME, item)

    def get_errors_by_file(self, file_id: str) -> List[Dict[str, Any]]:
        """Get all errors for a specific file"""
        from boto3.dynamodb.conditions import Key

        return self.db.query(
            self.TABLE_NAME,
            Key('file_id').eq(file_id)
        )

    def get_error(self, file_id: str, error_id: str) -> Dict[str, Any]:
        """Get a specific error"""
        return self.db.get_item(
            self.TABLE_NAME,
            {'file_id': file_id, 'error_id': error_id}
        )

    def get_errors_by_type(self, error_type: str) -> List[Dict[str, Any]]:
        """Get all errors of a specific type"""
        return self.db.scan(
            self.TABLE_NAME,
            FilterExpression='error_type = :error_type',
            ExpressionAttributeValues={':error_type': error_type}
        )
