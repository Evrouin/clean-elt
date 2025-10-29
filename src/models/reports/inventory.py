from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, field_validator
from decimal import Decimal
import re
from .base import BaseReport


class InventoryReportData(BaseModel):
    """Inventory report data model with field validations"""

    item_id: str
    item_name: str
    category: str
    quantity_on_hand: int
    reorder_level: int
    last_updated: datetime
    cost_price: Optional[Decimal] = None
    status: Optional[str] = "ACTIVE"

    @field_validator('item_id')
    @classmethod
    def validate_item_id(cls, v):
        if not re.match(r'^ITEM-\d{3,}$', v):
            raise ValueError('Item ID must follow format ITEM-NNN (minimum 3 digits)')
        return v

    @field_validator('item_name')
    @classmethod
    def validate_item_name_not_empty(cls, v):
        if not v or v.strip() == '':
            raise ValueError('Item name cannot be empty')
        return v.strip()

    @field_validator('category')
    @classmethod
    def validate_category(cls, v):
        valid_categories = ["AUTO_PARTS", "ELECTRONICS", "FOOD", "CLOTHING", "MISC"]
        if v not in valid_categories:
            raise ValueError(f'Category must be one of: {valid_categories}')
        return v

    @field_validator('quantity_on_hand')
    @classmethod
    def validate_quantity_non_negative(cls, v):
        if v < 0:
            raise ValueError('Quantity on hand cannot be negative')
        return v

    @field_validator('reorder_level')
    @classmethod
    def validate_reorder_level_non_negative(cls, v):
        if v < 0:
            raise ValueError('Reorder level cannot be negative')
        return v

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        if v:
            valid_statuses = ["ACTIVE", "DISCONTINUED"]
            if v not in valid_statuses:
                raise ValueError(f'Status must be one of: {valid_statuses}')
        return v


class InventoryReport(BaseReport):
    """Inventory report model"""

    def get_validation_rules(self) -> Dict[str, Any]:
        return {
            "max_update_age_hours": 24,
            "restock_alert_threshold": "quantity_on_hand < reorder_level",
            "reference_tables": {
                "inventory_master": "item_id"
            },
            "business_rules": [
                "check_negative_stock",
                "validate_restock_alerts",
                "reconcile_inventory_value",
                "validate_discontinued_items"
            ]
        }

    def transform_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        # Transform raw CSV/JSON data to match InventoryReportData model
        transformed = {}

        # Map common field variations
        field_mappings = {
            'id': 'item_id',
            'product_id': 'item_id',
            'name': 'item_name',
            'product_name': 'item_name',
            'type': 'category',
            'stock': 'quantity_on_hand',
            'qty': 'quantity_on_hand',
            'min_stock': 'reorder_level',
            'reorder_point': 'reorder_level',
            'updated': 'last_updated',
            'timestamp': 'last_updated',
            'price': 'cost_price',
            'unit_cost': 'cost_price'
        }

        for raw_key, raw_value in raw_data.items():
            # Use mapping if exists, otherwise use original key
            model_key = field_mappings.get(raw_key.lower(), raw_key)
            transformed[model_key] = raw_value

        return transformed
