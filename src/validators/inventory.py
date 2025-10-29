from typing import Dict, Any, List
from decimal import Decimal
from src.models.reports.inventory import InventoryReportData
from pydantic import ValidationError


class InventoryValidator:
    """Inventory report validator with field and business rules"""

    def validate(self, data: Dict[str, Any]) -> List[str]:
        errors = []

        # Step 1: Pydantic field validation
        try:
            inventory_data = InventoryReportData(**data)
        except ValidationError as e:
            for error in e.errors():
                field = error['loc'][0] if error['loc'] else 'unknown'
                message = error['msg']
                errors.append(f"Field '{field}': {message}")
            return errors  # Return early if field validation fails

        # Step 2: Business validation rules
        errors.extend(self._validate_business_rules(inventory_data, data))

        return errors

    def _validate_business_rules(self, inventory_data: InventoryReportData, raw_data: Dict[str, Any]) -> List[str]:
        """Validate business process rules"""
        errors = []

        # Business Rule: Check for negative stock (should never happen)
        if inventory_data.quantity_on_hand < 0:
            errors.append(f"Item {inventory_data.item_id} has negative stock: {inventory_data.quantity_on_hand}")

        # Business Rule: Restock alert for low inventory
        if inventory_data.quantity_on_hand < inventory_data.reorder_level:
            errors.append(f"RESTOCK ALERT: Item {inventory_data.item_id} stock ({inventory_data.quantity_on_hand}) below reorder level ({inventory_data.reorder_level})")

        # Business Rule: Discontinued items should have zero stock
        if inventory_data.status == "DISCONTINUED" and inventory_data.quantity_on_hand > 0:
            errors.append(f"Discontinued item {inventory_data.item_id} has non-zero stock: {inventory_data.quantity_on_hand}")

        # Business Rule: Validate inventory value calculation (if cost_price provided)
        if inventory_data.cost_price and 'expected_total_value' in raw_data:
            calculated_value = inventory_data.quantity_on_hand * inventory_data.cost_price
            expected_value = Decimal(str(raw_data['expected_total_value']))
            if abs(calculated_value - expected_value) > Decimal('0.01'):
                errors.append(f"Inventory value mismatch: calculated {calculated_value}, expected {expected_value}")

        return errors

    def get_required_fields(self) -> List[str]:
        return [
            'item_id',
            'item_name',
            'category',
            'quantity_on_hand',
            'reorder_level',
            'last_updated'
        ]
