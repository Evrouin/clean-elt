from pydantic import BaseModel
from typing import List, Dict, Any


class S3EventRecord(BaseModel):
    """S3 event record from SQS message"""
    bucket: str
    key: str
    event_name: str
    event_time: str
    size: int = 0

    @classmethod
    def from_s3_record(cls, s3_record: Dict[str, Any]) -> 'S3EventRecord':
        """Create from raw S3 event record"""
        return cls(
            bucket=s3_record['s3']['bucket']['name'],
            key=s3_record['s3']['object']['key'],
            event_name=s3_record['eventName'],
            event_time=s3_record['eventTime'],
            size=s3_record['s3']['object'].get('size', 0)
        )


class SQSRequest(BaseModel):
    """Parsed SQS request containing S3 events"""
    records: List[S3EventRecord]
    request_id: str

    @classmethod
    def from_sqs_event(cls, event: Dict[str, Any], context: Any) -> 'SQSRequest':
        """Parse SQS event into structured request"""
        import json

        records = []
        for sqs_record in event['Records']:
            # Parse SQS message body (contains S3 event)
            s3_event = json.loads(sqs_record['body'])

            for s3_record in s3_event['Records']:
                records.append(S3EventRecord.from_s3_record(s3_record))

        return cls(
            records=records,
            request_id=getattr(context, 'aws_request_id', 'unknown')
        )
