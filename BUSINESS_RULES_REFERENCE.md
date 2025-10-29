# 🧾 Business Rules Reference

> **⚠️ SAMPLE RULES DISCLAIMER**
> The validation rules documented below are **sample implementations** for demonstration purposes. In production environments, validation rules should be:
> - Customized to match your specific business requirements
> - Reviewed and approved by business stakeholders
> - Regularly updated to reflect changing business logic
> - Tested thoroughly before deployment

This document outlines the **validation rules** for three sample report types — **Sales**, **Expense**, and **Inventory** — demonstrating both **Field Validation** and **Business Logic Validation** capabilities.

---

## 🏗️ Validation Architecture

### Two-Layer Validation System
1. **Field Validation (Schema-based)**: Structural data integrity using Pydantic models
2. **Business Rules Validation (Dynamic)**: Configurable business logic stored in DynamoDB

### Rule Storage & Execution
- **Field Rules**: Defined in Pydantic models (`src/models/reports/`)
- **Business Rules**: Stored in DynamoDB `validation-rules` table
- **Rule Types**: RANGE, COMPARISON, BUSINESS (custom expressions)

---

## 1. Sales Report Validation Rules

### A. Field Validation Rules (Pydantic Schema)
| Field | Rule | Example | Error Type |
|--------|------|----------|------------|
| `transaction_id` | Required, string, max 50 chars | `TXN-20251029001` | CRITICAL |
| `transaction_date` | Valid ISO date, not future | `2025-10-28T14:30:00` | CRITICAL |
| `product_id` | Required, alphanumeric | `PROD-102` | CRITICAL |
| `quantity` | Positive integer > 0 | `3` | CRITICAL |
| `unit_price` | Positive decimal > 0 | `99.99` | CRITICAL |
| `total_amount` | Positive decimal | `299.97` | CRITICAL |

### B. Business Process Validation Rules (DynamoDB)
| Rule ID | Description | Expression | Severity |
|---------|-------------|------------|----------|
| `SALES_001` | Total amount must equal quantity × unit_price | `total_amount == quantity * unit_price` | CRITICAL |
| `SALES_002` | Quantity must not exceed 1000 per transaction | `quantity <= 1000` | WARNING |
| `SALES_003` | Unit price must be reasonable (< ₱100,000) | `unit_price < 100000` | WARNING |
| `SALES_004` | Transaction date within last 30 days | `transaction_date >= today() - 30` | INFO |

---

## 2. Expense Report Validation Rules

### A. Field Validation Rules (Pydantic Schema)
| Field | Rule | Example | Error Type |
|--------|------|----------|------------|
| `expense_id` | Required, unique identifier | `EXP-20251029-001` | CRITICAL |
| `expense_date` | Valid date, not future | `2025-10-27` | CRITICAL |
| `category` | Enum: SUPPLIES, UTILITIES, RENT, SALARY, MISC | `SUPPLIES` | CRITICAL |
| `amount` | Positive decimal > 0 | `1250.50` | CRITICAL |
| `employee_id` | Required, alphanumeric | `EMP002` | CRITICAL |
| `description` | Required, min 5 chars | `Office cleaning supplies` | WARNING |

### B. Business Process Validation Rules (DynamoDB)
| Rule ID | Description | Expression | Severity |
|---------|-------------|------------|----------|
| `EXPENSE_001` | Amount must be reasonable (< ₱500,000) | `amount < 500000` | CRITICAL |
| `EXPENSE_002` | Salary expenses only on payroll dates | `category != 'SALARY' OR is_payroll_date(expense_date)` | WARNING |
| `EXPENSE_003` | Description required for amounts > ₱10,000 | `amount <= 10000 OR len(description) >= 20` | WARNING |
| `EXPENSE_004` | No duplicate expenses same day/amount | `unique(expense_date, amount, employee_id)` | INFO |

---

## 3. Inventory Report Validation Rules

### A. Field Validation Rules (Pydantic Schema)
| Field | Rule | Example | Error Type |
|--------|------|----------|------------|
| `product_id` | Required, unique identifier | `ITEM-101` | CRITICAL |
| `product_name` | Required, min 3 chars | `Brake Pads` | CRITICAL |
| `category` | Enum: AUTO_PARTS, ELECTRONICS, FOOD, etc. | `AUTO_PARTS` | CRITICAL |
| `quantity_on_hand` | Non-negative integer | `56` | CRITICAL |
| `reorder_level` | Non-negative integer | `10` | WARNING |
| `last_updated` | Valid timestamp | `2025-10-29T09:12:00` | WARNING |

### B. Business Process Validation Rules (DynamoDB)
| Rule ID | Description | Expression | Severity |
|---------|-------------|------------|----------|
| `INVENTORY_001` | Quantity cannot be negative | `quantity_on_hand >= 0` | CRITICAL |
| `INVENTORY_002` | Reorder level should be reasonable | `reorder_level >= 0 AND reorder_level <= 1000` | WARNING |
| `INVENTORY_003` | Low stock alert trigger | `quantity_on_hand >= reorder_level` | INFO |
| `INVENTORY_004` | Recent update requirement | `last_updated >= today() - 7` | WARNING |
| `INVENTORY_005` | Discontinued items should have zero stock | `status != 'DISCONTINUED' OR quantity_on_hand == 0` | INFO |
| `INVENTORY_006` | Reasonable quantity limits | `quantity_on_hand <= 10000` | WARNING |

---

## 🔧 Implementation Details

### Field Validation (Pydantic Models)
```python
# Example: Sales Report Model
class SalesReport(BaseModel):
    transaction_id: str = Field(..., max_length=50)
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., gt=0)
    total_amount: Decimal = Field(..., gt=0)
```

### Business Rules (DynamoDB Storage)
```json
{
  "rule_id": "SALES_001",
  "report_type": "SALES",
  "rule_type": "COMPARISON",
  "expression": "total_amount == quantity * unit_price",
  "severity": "CRITICAL",
  "error_message": "Total amount must equal quantity × unit price",
  "is_active": true
}
```

### Rule Execution Engine
- **Pre-compilation**: Rules compiled at startup for performance
- **Fast-fail**: Critical errors stop processing immediately
- **Batch processing**: Rules executed in priority order (CRITICAL → WARNING → INFO)

---

## 📊 Validation Results Structure

### Validation Response Format
```json
{
  "field_errors": [
    {
      "field": "quantity",
      "error": "must be greater than 0",
      "severity": "CRITICAL"
    }
  ],
  "business_violations": [
    {
      "rule_id": "SALES_001",
      "error": "Total amount must equal quantity × unit price",
      "severity": "CRITICAL"
    }
  ],
  "is_valid": false
}
```