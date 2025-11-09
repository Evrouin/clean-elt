import json
import csv
import io
from typing import List, Dict, Any


class FileParser:
    """Utility class for parsing different file formats"""

    @staticmethod
    def parse_file_data(file_data: str, key: str) -> List[Dict[str, Any]]:
        """Parse file data based on file extension"""
        if key.lower().endswith('.csv'):
            return FileParser.parse_csv(file_data)
        elif key.lower().endswith('.json'):
            return FileParser.parse_json(file_data)
        else:
            raise ValueError(f"Unsupported file format: {key}")

    @staticmethod
    def parse_csv(file_data: str) -> List[Dict[str, Any]]:
        """Parse CSV file data"""
        csv_reader = csv.DictReader(io.StringIO(file_data))
        return list(csv_reader)

    @staticmethod
    def parse_json(file_data: str) -> List[Dict[str, Any]]:
        """Parse JSON file data"""
        data = json.loads(file_data)
        return data if isinstance(data, list) else [data]
