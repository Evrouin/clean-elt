from pydantic import BaseModel
from typing import List, Optional


class FileProcessingResult(BaseModel):
    """Result of processing a single file"""
    report_type: str
    status: str
    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    file_location: str
    error: Optional[str] = None


class ProcessingResponse(BaseModel):
    """Response from report processing"""
    message: str
    results: List[FileProcessingResult]
    total_files: int
    successful_files: int
    failed_files: int

    @classmethod
    def create_success(cls, results: List[FileProcessingResult]) -> 'ProcessingResponse':
        """Create successful response"""
        successful = len([r for r in results if r.error is None])
        failed = len(results) - successful

        return cls(
            message="File processing completed",
            results=results,
            total_files=len(results),
            successful_files=successful,
            failed_files=failed
        )

    @classmethod
    def create_error(cls, error_message: str) -> 'ProcessingResponse':
        """Create error response"""
        return cls(
            message="Processing failed",
            results=[],
            total_files=0,
            successful_files=0,
            failed_files=0
        )
