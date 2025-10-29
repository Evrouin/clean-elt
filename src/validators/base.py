from abc import ABC, abstractmethod
from typing import Dict, Any, List


class BaseValidator(ABC):
    """Abstract base class for all validators"""

    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> List[str]:
        """
        Validate data and return list of error messages
        Returns empty list if validation passes
        """
        pass

    @abstractmethod
    def get_required_fields(self) -> List[str]:
        """Return list of required fields for this report type"""
        pass
