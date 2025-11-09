from typing import List, Dict, Any
from datetime import datetime, timedelta
from src.services.aws.dynamodb_service import DynamoDBService


class FileProcessingLog:
    """Model for file-processing-log DynamoDB table"""

    TABLE_NAME = 'file-processing-log'

    def __init__(self, dynamodb_service: DynamoDBService = None):
        self.db = dynamodb_service or DynamoDBService()

    def log_processing(self, file_id: str, status: str, total_rows: int, valid_rows: int, invalid_rows: int, **metadata):
        """Log file processing summary"""
        item = {
            'file_id': file_id,
            'timestamp': datetime.now().isoformat(),
            'status': status,
            'total_rows': total_rows,
            'valid_rows': valid_rows,
            'invalid_rows': invalid_rows,
            'ttl': int((datetime.now() + timedelta(days=90)).timestamp()),
            **metadata
        }

        self.db.put_item(self.TABLE_NAME, item)

    def get_processing_log(self, file_id: str) -> Dict[str, Any]:
        """Get processing log for a file"""
        return self.db.get_item(
            self.TABLE_NAME,
            {'file_id': file_id}
        )

    def get_processing_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent processing history"""
        return self.db.scan(
            self.TABLE_NAME,
            Limit=limit
        )

    def get_failed_processing(self) -> List[Dict[str, Any]]:
        """Get all failed processing attempts"""
        return self.db.scan(
            self.TABLE_NAME,
            FilterExpression='#status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={':status': 'error'}
        )
