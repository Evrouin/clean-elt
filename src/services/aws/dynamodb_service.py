import boto3
import os
from typing import Dict, Any, List


class DynamoDBService:
    """Generic DynamoDB service for basic operations"""

    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.stage = os.environ.get('STAGE', 'dev')

    def get_table(self, table_name: str):
        """Get DynamoDB table reference"""
        return self.dynamodb.Table(f'{table_name}-{self.stage}')

    def put_item(self, table_name: str, item: Dict[str, Any]):
        """Put item to table"""
        try:
            table = self.get_table(table_name)
            table.put_item(Item=item)
        except Exception as e:
            print(f"Error putting item to {table_name}: {e}")
            raise

    def get_item(self, table_name: str, key: Dict[str, Any]) -> Dict[str, Any]:
        """Get item from table"""
        try:
            table = self.get_table(table_name)
            response = table.get_item(Key=key)
            return response.get('Item', {})
        except Exception as e:
            print(f"Error getting item from {table_name}: {e}")
            return {}

    def query(self, table_name: str, key_condition: Any, **kwargs) -> List[Dict[str, Any]]:
        """Query table with key condition"""
        try:
            table = self.get_table(table_name)
            response = table.query(KeyConditionExpression=key_condition, **kwargs)
            return response.get('Items', [])
        except Exception as e:
            print(f"Error querying {table_name}: {e}")
            return []

    def scan(self, table_name: str, **kwargs) -> List[Dict[str, Any]]:
        """Scan table"""
        try:
            table = self.get_table(table_name)
            response = table.scan(**kwargs)
            return response.get('Items', [])
        except Exception as e:
            print(f"Error scanning {table_name}: {e}")
            return []

    def update_item(self, table_name: str, key: Dict[str, Any],
                    update_expression: str, expression_values: Dict[str, Any]):
        """Update item in table"""
        try:
            table = self.get_table(table_name)
            table.update_item(
                Key=key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values
            )
        except Exception as e:
            print(f"Error updating item in {table_name}: {e}")
            raise

    def delete_item(self, table_name: str, key: Dict[str, Any]):
        """Delete item from table"""
        try:
            table = self.get_table(table_name)
            table.delete_item(Key=key)
        except Exception as e:
            print(f"Error deleting item from {table_name}: {e}")
            raise
