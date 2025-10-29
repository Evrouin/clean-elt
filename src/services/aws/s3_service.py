import boto3
from botocore.exceptions import ClientError
from src.utils.logger import StructuredLogger


class S3Service:
    """Generic S3 service with standard S3 operations"""

    def __init__(self):
        self.client = boto3.client('s3')
        self.logger = StructuredLogger(__name__)

    def get_object(self, bucket: str, key: str) -> dict:
        """Get object from S3"""
        try:
            return self.client.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            self.logger.error(f"Failed to get object: {e}")
            raise

    def get_object_content(self, bucket: str, key: str) -> str:
        """Get object content as string"""
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response['Body'].read().decode('utf-8')
        except ClientError as e:
            self.logger.error(f"Failed to get object content: {e}")
            raise

    def put_object(self, bucket: str, key: str, body: bytes, content_type: str = None) -> dict:
        """Put object to S3"""
        try:
            params = {'Bucket': bucket, 'Key': key, 'Body': body}
            if content_type:
                params['ContentType'] = content_type
            return self.client.put_object(**params)
        except ClientError as e:
            self.logger.error(f"Failed to put object: {e}")
            raise

    def delete_object(self, bucket: str, key: str) -> dict:
        """Delete object from S3"""
        try:
            return self.client.delete_object(Bucket=bucket, Key=key)
        except ClientError as e:
            self.logger.error(f"Failed to delete object: {e}")
            raise

    def head_object(self, bucket: str, key: str) -> dict:
        """Get object metadata"""
        try:
            return self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            self.logger.error(f"Failed to get object metadata: {e}")
            raise

    def list_objects(self, bucket: str, prefix: str = None, max_keys: int = 1000) -> dict:
        """List objects in bucket"""
        try:
            params = {'Bucket': bucket, 'MaxKeys': max_keys}
            if prefix:
                params['Prefix'] = prefix
            return self.client.list_objects_v2(**params)
        except ClientError as e:
            self.logger.error(f"Failed to list objects: {e}")
            raise

    def copy_object(self, source_bucket: str, source_key: str, dest_bucket: str, dest_key: str) -> dict:
        """Copy object within S3"""
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            return self.client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
        except ClientError as e:
            self.logger.error(f"Failed to copy object: {e}")
            raise

    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists"""
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
