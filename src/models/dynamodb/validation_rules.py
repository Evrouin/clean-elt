from typing import List, Dict, Any
from datetime import datetime
from src.services.aws.dynamodb_service import DynamoDBService


class ValidationRules:
    """Model for validation-rules DynamoDB table"""

    TABLE_NAME = 'validation-rules'

    def __init__(self, dynamodb_service: DynamoDBService = None):
        self.db = dynamodb_service or DynamoDBService()

    def get_rules_by_report_type(self, report_type: str) -> List[Dict[str, Any]]:
        """Get all validation rules for a specific report type"""
        from boto3.dynamodb.conditions import Key

        return self.db.query(
            self.TABLE_NAME,
            Key('report_type').eq(report_type)
        )

    def get_rule(self, report_type: str, rule_id: str) -> Dict[str, Any]:
        """Get a specific validation rule"""
        return self.db.get_item(
            self.TABLE_NAME,
            {'report_type': report_type, 'rule_id': rule_id}
        )

    def create_rule(self, report_type: str, rule_id: str, rule_name: str, rule_expression: str, validation_type: str = 'BUSINESS', severity: str = 'ERROR', description: str = ''):
        """Create a new validation rule"""
        item = {
            'report_type': report_type,
            'rule_id': rule_id,
            'rule_name': rule_name,
            'rule_expression': rule_expression,
            'validation_type': validation_type,
            'severity': severity,
            'description': description,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }

        self.db.put_item(self.TABLE_NAME, item)

    def update_rule(self, report_type: str, rule_id: str, **updates):
        """Update an existing validation rule"""
        updates['updated_at'] = datetime.now().isoformat()

        update_expression = "SET " + ", ".join([f"{k} = :{k}" for k in updates.keys()])
        expression_values = {f":{k}": v for k, v in updates.items()}

        self.db.update_item(
            self.TABLE_NAME,
            {'report_type': report_type, 'rule_id': rule_id},
            update_expression,
            expression_values
        )

    def delete_rule(self, report_type: str, rule_id: str):
        """Delete a validation rule"""
        self.db.delete_item(
            self.TABLE_NAME,
            {'report_type': report_type, 'rule_id': rule_id}
        )
