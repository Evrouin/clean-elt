from typing import Dict, Any, Optional
from datetime import date
from pydantic import BaseModel, field_validator
from decimal import Decimal
import re
from .base import BaseReport


class ExpenseReportData(BaseModel):
    """Expense report data model with field validations"""

    expense_id: str
    date: date
    category: str
    amount: Decimal
    description: str
    approved_by: str
    justification: Optional[str] = None

    @field_validator('expense_id')
    @classmethod
    def validate_expense_id(cls, v):
        if not re.match(r'^EXP-\d{8}-\d{3}$', v):
            raise ValueError('Expense ID must follow format EXP-YYYYMMDD-NNN')
        return v

    @field_validator('date')
    @classmethod
    def validate_date_not_future(cls, v):
        if v > date.today():
            raise ValueError('Expense date cannot be in the future')
        return v

    @field_validator('category')
    @classmethod
    def validate_category(cls, v):
        valid_categories = ["SUPPLIES", "UTILITIES", "RENT", "SALARY", "MISC"]
        if v not in valid_categories:
            raise ValueError(f'Category must be one of: {valid_categories}')
        return v

    @field_validator('amount')
    @classmethod
    def validate_amount_positive(cls, v):
        if v <= 0:
            raise ValueError('Amount must be positive')
        return v

    @field_validator('description')
    @classmethod
    def validate_description_not_empty(cls, v):
        if not v or v.strip() == '':
            raise ValueError('Description cannot be empty')
        return v.strip()


class ExpenseReport(BaseReport):
    """Expense report model"""

    def get_validation_rules(self) -> Dict[str, Any]:
        return {
            "large_expense_threshold": 10000,
            "max_duplicate_check_days": 30,
            "reference_tables": {
                "employee_master": "approved_by"
            },
            "business_rules": [
                "check_budget_limits",
                "prevent_duplicates",
                "validate_payroll_period",
                "validate_approver_authority"
            ]
        }

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        # Transform raw CSV/JSON data to match ExpenseReportData model
        transformed = {}

        # Map common field variations
        field_mappings = {
            'exp_id': 'expense_id',
            'expense_type': 'category',
            'cost': 'amount',
            'total': 'amount',
            'desc': 'description',
            'approver': 'approved_by',
            'approved_by_id': 'approved_by',
            'reason': 'justification'
        }

        for raw_key, raw_value in raw_data.items():
            # Use mapping if exists, otherwise use original key
            model_key = field_mappings.get(raw_key.lower(), raw_key)
            transformed[model_key] = raw_value

        return transformed
