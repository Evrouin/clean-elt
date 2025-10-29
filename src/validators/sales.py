from typing import Dict, Any, List
from decimal import Decimal
from src.models.reports.sales import SalesReportData
from pydantic import ValidationError


class SalesValidator:
    """Sales report validator with field and business rules"""

    def validate(self, data: Dict[str, Any]) -> List[str]:
        errors = []

        # Step 1: Pydantic field validation
        try:
            sales_data = SalesReportData(**data)
        except ValidationError as e:
            for error in e.errors():
                field = error['loc'][0] if error['loc'] else 'unknown'
                message = error['msg']
                errors.append(f"Field '{field}': {message}")
            return errors  # Return early if field validation fails

        # Step 2: Business validation rules
        errors.extend(self._validate_business_rules(sales_data, data))

        return errors

    def _validate_business_rules(self, sales_data: SalesReportData, raw_data: Dict[str, Any]) -> List[str]:
        """Validate business process rules"""
        errors = []

        # Business Rule: Check discount limits (if discount field exists)
        if 'discount_amount' in raw_data:
            discount = Decimal(str(raw_data['discount_amount']))
            discount_percentage = (discount / sales_data.total_amount) * 100
            if discount_percentage > 50:
                errors.append(f"Discount {discount_percentage:.1f}% exceeds maximum allowed 50%")

        # Business Rule: Validate refund transactions
        if sales_data.total_amount < 0:  # Negative amount indicates refund
            if 'reference_transaction_id' not in raw_data:
                errors.append("Refund transactions must include reference_transaction_id")

        return errors

    def get_required_fields(self) -> List[str]:
        return [
            'transaction_id',
            'date',
            'customer_id',
            'item_id',
            'quantity',
            'unit_price',
            'total_amount',
            'payment_method'
        ]
