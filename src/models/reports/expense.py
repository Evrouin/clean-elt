from typing import Dict, Any, Optional
from datetime import date
from pydantic import BaseModel, field_validator, model_validator
from decimal import Decimal
import re


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

    @field_validator('approved_by')
    @classmethod
    def validate_approved_by(cls, v):
        # Mock employee validation - simulate employee_master lookup
        if not re.match(r'^EMP\d{3,}$', v):
            raise ValueError('Approver ID must follow format EMPXXX')
        # Simulate unauthorized approvers
        unauthorized_approvers = ['EMP999', 'EMP888', 'EMP777']
        if v in unauthorized_approvers:
            raise ValueError(f'Employee {v} is not authorized to approve expenses')
        return v

    @model_validator(mode='after')
    def validate_large_expense_justification(self):
        """Validate expenses > 10000 have justification"""
        if self.amount > 10000:
            if not self.justification or self.justification.strip() == '':
                raise ValueError('Expenses above ₱10,000 require justification')
        return self

    @model_validator(mode='after')
    def validate_salary_timing(self):
        """Validate salary expenses occur during payroll periods"""
        if self.category == "SALARY":
            # Simulate payroll periods: 1-15 and 16-31 of each month
            if not (1 <= self.date.day <= 15 or 16 <= self.date.day <= 31):
                raise ValueError('Salary expenses should occur during payroll periods (1-15 or 16-31)')
        return self


class ExpenseReport(BaseModel):
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
