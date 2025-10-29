import boto3
from typing import Dict, Any


class S3Service:
    """S3 service wrapper for common operations"""

    def __init__(self):
        self.client = boto3.client('s3')

    def get_object_content(self, bucket: str, key: str) -> str:
        """Get object content as string"""
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read().decode('utf-8')
        except Exception as e:
            raise Exception(f"Error reading S3 object {bucket}/{key}: {str(e)}")

    def get_object_metadata(self, bucket: str, key: str) -> Dict[str, Any]:
        """Get object metadata"""
        try:
            response = self.client.head_object(Bucket=bucket, Key=key)
            return {
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType', ''),
                'etag': response.get('ETag', '')
            }
        except Exception as e:
            raise Exception(f"Error getting S3 metadata {bucket}/{key}: {str(e)}")

    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists"""
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False
