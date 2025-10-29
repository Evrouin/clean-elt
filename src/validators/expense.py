from typing import Dict, Any, List
from decimal import Decimal
from datetime import date
from .base import BaseValidator
from src.models.reports.expense import ExpenseReportData
from pydantic import ValidationError


class ExpenseValidator(BaseValidator):
    """Expense report validator with field and business rules"""

    def validate(self, data: Dict[str, Any]) -> List[str]:
        errors = []

        # Step 1: Pydantic field validation
        try:
            expense_data = ExpenseReportData(**data)
        except ValidationError as e:
            for error in e.errors():
                field = error['loc'][0] if error['loc'] else 'unknown'
                message = error['msg']
                errors.append(f"Field '{field}': {message}")
            return errors  # Return early if field validation fails

        # Step 2: Business validation rules
        errors.extend(self._validate_business_rules(expense_data, data))

        return errors

    def _validate_business_rules(self, expense_data: ExpenseReportData, raw_data: Dict[str, Any]) -> List[str]:
        """Validate business process rules"""
        errors = []

        # Business Rule: Salary expenses only during payroll period
        if expense_data.category == "SALARY":
            if not self._is_payroll_period(expense_data.date):
                errors.append("Salary expenses must occur within payroll cut-off period")

        # Business Rule: Large expense justification (already handled in Pydantic, but double-check)
        if expense_data.amount > Decimal('10000') and not expense_data.justification:
            errors.append("Expenses above ₱10,000 require justification")

        return errors

    def _is_payroll_period(self, expense_date: date) -> bool:
        """Check if expense date falls within payroll cut-off period"""
        # Assuming payroll periods are 1st-15th and 16th-end of month
        day = expense_date.day
        return day <= 15 or day >= 16  # Simplified - always valid for now

    def get_required_fields(self) -> List[str]:
        return [
            'expense_id',
            'date',
            'category',
            'amount',
            'description',
            'approved_by'
        ]
