# Business Rules Reference

This document defines validation rules for CleanELT's data quality validation system. Rules are organized by report type and validation layer.

> **Note**: These are sample rules for demonstration. Production rules should be customized to match specific business requirements and approved by stakeholders.

## Validation Architecture

### Two-Layer System
1. **Field Validation**: Schema-based structural validation using Pydantic models
2. **Business Rules**: Dynamic business logic stored in DynamoDB

### Rule Types
- **RANGE**: Numeric range validation
- **COMPARISON**: Field comparison validation
- **BUSINESS**: Custom business logic expressions

### Severity Levels
- **CRITICAL**: Processing stops, data rejected
- **WARNING**: Processing continues, issue logged
- **INFO**: Processing continues, informational only

## Sales Report Rules

### Field Validation
| Field | Type | Constraints | Example |
|-------|------|-------------|---------|
| transaction_id | String | Required, max 50 chars | `TXN-20251029001` |
| transaction_date | DateTime | Required, not future | `2025-10-28T14:30:00` |
| product_id | String | Required, alphanumeric | `PROD-102` |
| quantity | Integer | Required, > 0 | `3` |
| unit_price | Decimal | Required, > 0 | `99.99` |
| total_amount | Decimal | Required, > 0 | `299.97` |

### Business Rules
| Rule ID | Description | Expression | Severity |
|---------|-------------|------------|----------|
| SALES_001 | Amount calculation accuracy | `total_amount == quantity * unit_price` | CRITICAL |
| SALES_002 | Quantity limit per transaction | `quantity <= 1000` | WARNING |
| SALES_003 | Unit price reasonableness | `unit_price < 100000` | WARNING |
| SALES_004 | Transaction date recency | `transaction_date >= today() - 30` | INFO |
| SALES_005 | Minimum transaction value | `total_amount >= 1.00` | WARNING |
| SALES_006 | Product ID format validation | `product_id.startswith('PROD-')` | INFO |

## Expense Report Rules

### Field Validation
| Field | Type | Constraints | Example |
|-------|------|-------------|---------|
| expense_id | String | Required, unique | `EXP-20251029-001` |
| expense_date | Date | Required, not future | `2025-10-27` |
| category | Enum | SUPPLIES, UTILITIES, RENT, SALARY, MISC | `SUPPLIES` |
| amount | Decimal | Required, > 0 | `1250.50` |
| employee_id | String | Required, alphanumeric | `EMP002` |
| description | String | Required, min 5 chars | `Office supplies` |

### Business Rules
| Rule ID | Description | Expression | Severity |
|---------|-------------|------------|----------|
| EXPENSE_001 | Maximum expense amount | `amount < 500000` | CRITICAL |
| EXPENSE_002 | Salary expense timing | `category != 'SALARY' OR is_payroll_date(expense_date)` | WARNING |
| EXPENSE_003 | High-value description requirement | `amount <= 10000 OR len(description) >= 20` | WARNING |
| EXPENSE_004 | Duplicate expense prevention | `unique(expense_date, amount, employee_id)` | INFO |

## Inventory Report Rules

### Field Validation
| Field | Type | Constraints | Example |
|-------|------|-------------|---------|
| product_id | String | Required, unique | `ITEM-101` |
| product_name | String | Required, min 3 chars | `Brake Pads` |
| category | Enum | AUTO_PARTS, ELECTRONICS, FOOD, etc. | `AUTO_PARTS` |
| quantity_on_hand | Integer | Required, >= 0 | `56` |
| reorder_level | Integer | Required, >= 0 | `10` |
| last_updated | DateTime | Required | `2025-10-29T09:12:00` |

### Business Rules
| Rule ID | Description | Expression | Severity |
|---------|-------------|------------|----------|
| INVENTORY_001 | Non-negative quantity | `quantity_on_hand >= 0` | CRITICAL |
| INVENTORY_002 | Reasonable reorder level | `reorder_level >= 0 AND reorder_level <= 1000` | WARNING |
| INVENTORY_003 | Low stock detection | `quantity_on_hand >= reorder_level` | INFO |
| INVENTORY_004 | Data freshness requirement | `last_updated >= today() - 7` | WARNING |
| INVENTORY_005 | Discontinued item stock | `status != 'DISCONTINUED' OR quantity_on_hand == 0` | INFO |
| INVENTORY_006 | Maximum quantity limit | `quantity_on_hand <= 10000` | WARNING |

## Implementation Examples

### Field Validation (Pydantic)
```python
class SalesReport(BaseModel):
    transaction_id: str = Field(..., max_length=50)
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., gt=0)
    total_amount: Decimal = Field(..., gt=0)
    transaction_date: datetime = Field(...)
```

### Business Rule (DynamoDB)
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

### Validation Response
```json
{
  "is_valid": false,
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
      "severity": "CRITICAL",
      "actual_value": "299.98",
      "expected_value": "299.97"
    }
  ],
  "summary": {
    "total_rows": 100,
    "valid_rows": 95,
    "critical_errors": 3,
    "warnings": 2,
    "info_messages": 0
  }
}
```

## Rule Management

### Adding New Rules
1. Define rule in DynamoDB `validation-rules` table
2. Test rule expression with sample data
3. Deploy and monitor validation results
4. Update documentation

### Rule Expression Syntax
- **Comparison operators**: `==`, `!=`, `<`, `>`, `<=`, `>=`
- **Logical operators**: `AND`, `OR`, `NOT`
- **Functions**: `today()`, `len()`, `startswith()`, `unique()`
- **Field references**: Use exact field names from data model

### Performance Considerations
- Rules are pre-compiled at startup for faster execution
- Critical rules execute first (fast-fail processing)
- Rule cache maintains 85% hit ratio for optimal performance
- Complex expressions should be tested for performance impact

## Troubleshooting

### Common Issues
| Issue | Cause | Solution |
|-------|-------|----------|
| Rule not executing | Inactive rule or syntax error | Check `is_active` flag and expression syntax |
| Performance degradation | Complex expressions | Simplify expressions or split into multiple rules |
| False positives | Incorrect expression logic | Review and test expression with edge cases |
| Missing validations | Rule not configured | Add rule to DynamoDB and verify deployment |

### Testing Rules
```python
# Test rule expression
expression = "total_amount == quantity * unit_price"
data = {"total_amount": 299.97, "quantity": 3, "unit_price": 99.99}
result = eval_expression(expression, data)  # Returns True/False
```
