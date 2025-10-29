from typing import Dict, Any
from datetime import date
from pydantic import BaseModel, field_validator, model_validator
from decimal import Decimal
import re


class SalesReportData(BaseModel):
    """Sales report data model with field validations"""

    transaction_id: str
    date: date
    customer_id: str
    item_id: str
    quantity: int
    unit_price: Decimal
    total_amount: Decimal
    payment_method: str

    @field_validator('transaction_id')
    @classmethod
    def validate_transaction_id(cls, v):
        if not re.match(r'^TXN-\d{8}\d{3}$', v):
            raise ValueError('Transaction ID must follow format TXN-YYYYMMDDNNN')
        return v

    @field_validator('date')
    @classmethod
    def validate_date_not_future(cls, v):
        if v > date.today():
            raise ValueError('Transaction date cannot be in the future')
        return v

    @field_validator('customer_id')
    @classmethod
    def validate_customer_id(cls, v):
        # Mock customer validation - simulate customer_master lookup
        if not re.match(r'^CUST\d{3,}$', v):
            raise ValueError('Customer ID must follow format CUSTXXX')
        # Simulate inactive customers
        inactive_customers = ['CUST999', 'CUST888', 'CUST777']
        if v in inactive_customers:
            raise ValueError(f'Customer {v} is inactive')
        return v

    @field_validator('item_id')
    @classmethod
    def validate_item_id(cls, v):
        # Mock inventory validation - simulate inventory_master lookup
        if not re.match(r'^ITEM-\d{3,}$', v):
            raise ValueError('Item ID must follow format ITEM-XXX')
        # Simulate non-existent items
        invalid_items = ['ITEM-999', 'ITEM-888', 'ITEM-777']
        if v in invalid_items:
            raise ValueError(f'Item {v} does not exist in inventory')
        return v

    @field_validator('quantity')
    @classmethod
    def validate_quantity_positive(cls, v):
        if v <= 0:
            raise ValueError('Quantity must be positive')
        return v

    @field_validator('unit_price')
    @classmethod
    def validate_unit_price_positive(cls, v):
        if v <= 0:
            raise ValueError('Unit price must be positive')
        return v

    @field_validator('total_amount')
    @classmethod
    def validate_total_amount_positive(cls, v):
        if v <= 0:
            raise ValueError('Total amount must be positive')
        return v

    @field_validator('payment_method')
    @classmethod
    def validate_payment_method(cls, v):
        valid_methods = ["CASH", "CARD", "E-WALLET"]
        if v not in valid_methods:
            raise ValueError(f'Payment method must be one of: {valid_methods}')
        return v

    @model_validator(mode='after')
    def validate_total_calculation(self):
        """Validate total_amount = quantity × unit_price"""
        expected_total = self.quantity * self.unit_price
        if abs(self.total_amount - expected_total) > Decimal('0.01'):
            raise ValueError('Total amount must equal quantity × unit price')
        return self


class SalesReport(BaseModel):
    """Sales report model"""

    def get_validation_rules(self) -> Dict[str, Any]:
        return {
            "max_discount_percentage": 50,
            "reference_tables": {
                "customer_master": "customer_id",
                "inventory_master": "item_id"
            },
            "business_rules": [
                "check_stock_availability",
                "validate_customer_active",
                "check_refund_reference",
                "validate_daily_totals"
            ]
        }

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        # Transform raw CSV/JSON data to match SalesReportData model
        transformed = {}

        # Map common field variations
        field_mappings = {
            'txn_id': 'transaction_id',
            'trans_id': 'transaction_id',
            'cust_id': 'customer_id',
            'product_id': 'item_id',
            'qty': 'quantity',
            'price': 'unit_price',
            'total': 'total_amount',
            'payment': 'payment_method'
        }

        for raw_key, raw_value in raw_data.items():
            # Use mapping if exists, otherwise use original key
            model_key = field_mappings.get(raw_key.lower(), raw_key)
            transformed[model_key] = raw_value

        return transformed
