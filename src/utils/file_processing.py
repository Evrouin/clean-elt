import csv
import io
from typing import Iterator, List, Dict, Any
from src.services.aws.s3_service import S3Service
from src.utils.error_logger import ErrorLogger
from src.utils.status_codes import ErrorCode, WarningCode


class FileProcessingUtils:
    """Utilities for file processing with memory optimization"""

    def __init__(self):
        self.s3_service = S3Service()
        self.logger = ErrorLogger(__name__)

    def stream_csv_batches(self, bucket: str, key: str, batch_size: int = 100) -> Iterator[List[Dict[str, Any]]]:
        """Stream CSV file in batches to minimize memory usage"""
        try:
            # Use streaming response to avoid loading entire file
            response = self.s3_service.get_object(bucket, key)

            # Stream the body content
            stream = response['Body']

            # Use text wrapper for CSV reading
            text_stream = io.TextIOWrapper(stream, encoding='utf-8')
            csv_reader = csv.DictReader(text_stream)

            batch = []
            for row in csv_reader:
                # Clean and optimize row data
                cleaned_row = self._clean_row_data(row)
                batch.append(cleaned_row)

                if len(batch) >= batch_size:
                    yield batch
                    batch = []  # Clear batch to free memory

            # Yield remaining rows
            if batch:
                yield batch

        except Exception as e:
            self.logger.log_file_error(
                ErrorCode.FILE_001,
                file_path=f"s3://{bucket}/{key}",
                exception=e,
                operation="csv_streaming",
            )
            raise
        finally:
            # Ensure stream is closed
            if 'stream' in locals():
                stream.close()

    def _clean_row_data(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Clean row data by removing empty values and converting types"""
        cleaned_data = {}

        for key, value in row.items():
            if value is not None and value != '':
                # Strip whitespace
                if isinstance(value, str):
                    value = value.strip()

                # Convert numeric strings to appropriate types
                if self._is_numeric(value):
                    try:
                        cleaned_data[key] = int(value) if '.' not in value else float(value)
                    except ValueError:
                        cleaned_data[key] = value
                else:
                    cleaned_data[key] = value

        return cleaned_data

    def _is_numeric(self, value: str) -> bool:
        """Quick numeric check without exception handling"""
        if not isinstance(value, str):
            return False

        # Remove common numeric formatting
        cleaned = value.replace(',', '').replace('$', '').strip()

        # Quick character-based check
        return cleaned.replace('.', '').replace('-', '').isdigit()

    def get_file_size(self, bucket: str, key: str) -> int:
        """Get file size without downloading content"""
        try:
            response = self.s3_service.head_object(bucket, key)
            return response['ContentLength']
        except Exception as e:
            self.logger.log_file_error(
                ErrorCode.FILE_002,
                file_path=f"s3://{bucket}/{key}",
                exception=e,
                operation="get_file_size",
            )
            return 0

    def estimate_processing_memory(self, bucket: str, key: str, batch_size: int = 100) -> Dict[str, int]:
        """Estimate memory requirements for processing"""
        file_size = self.get_file_size(bucket, key)

        # Rough estimates based on file size and batch size
        estimated_row_count = max(1, file_size // 100)  # Assume ~100 bytes per row, minimum 1
        batches_needed = max(1, (estimated_row_count // batch_size) + 1)

        # Memory estimates in MB (ensure minimum values)
        peak_memory_mb = max(1, (batch_size * 200) / (1024 * 1024))  # ~200 bytes per processed row
        total_memory_mb = max(2, peak_memory_mb * 1.5)  # Buffer for processing overhead

        return {
            'file_size_bytes': file_size,
            'estimated_rows': estimated_row_count,
            'batches_needed': batches_needed,
            'peak_memory_mb': int(peak_memory_mb),
            'recommended_memory_mb': int(total_memory_mb)
        }

    def get_safe_file_content(self, bucket: str, key: str, fallback_content: str = None) -> str:
        """Get file content with fallback for local testing"""
        try:
            return self.s3_service.get_object_content(bucket, key)
        except Exception as e:
            if fallback_content:
                self.logger.log_warning(
                    WarningCode.FILE_W101,
                    primary_source=f"s3://{bucket}/{key}",
                    fallback_available=True,
                    original_error=str(e),
                )
                return fallback_content
            raise
