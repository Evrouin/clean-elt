from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Dict, Any


class BaseReport(BaseModel, ABC):
    """Abstract base class for all report models"""

    file_name: str
    bucket: str
    key: str

    @abstractmethod
    def get_validation_rules(self) -> Dict[str, Any]:
        """Return validation rules for this report type"""
        pass

    @abstractmethod
    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data for validation"""
        pass
