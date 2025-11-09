import os
import boto3
from typing import Dict, Any


class RedshiftConfig:
    """Utility for managing Redshift configuration from environment and SSM"""

    @staticmethod
    def get_config() -> Dict[str, Any]:
        """Get Redshift configuration from environment variables"""
        return {
            'cluster_endpoint': os.getenv('REDSHIFT_HOST'),
            'database': os.getenv('REDSHIFT_DATABASE'),
            'user': os.getenv('REDSHIFT_USER'),
            'port': int(os.getenv('REDSHIFT_PORT'))
        }

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> bool:
        """Validate required Redshift configuration"""
        required_fields = ['cluster_endpoint', 'database', 'user']
        return all(config.get(field) for field in required_fields)

    @staticmethod
    def get_iam_role_arn() -> str:
        """Get IAM role ARN for COPY operations"""
        stage = os.getenv('STAGE', 'nonprod')
        ssm = boto3.client('ssm')

        try:
            response = ssm.get_parameter(
                Name=f'/{stage}/etl-data-quality-checker/EXECUTION_ROLE_ARN'
            )
            return response['Parameter']['Value']
        except Exception:
            account_id = boto3.client('sts').get_caller_identity()['Account']
            return f"arn:aws:iam::{account_id}:role/etl-data-quality-checker-{stage}-execution-role"
