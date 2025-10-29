from typing import Dict, List, Any
from decimal import Decimal
from src.models.dynamodb.validation_rules import ValidationRules


class BusinessRulesValidator:
    """Service to load and execute business rules from DynamoDB"""

    def __init__(self):
        self.validation_rules = ValidationRules()

    def get_rules(self, report_type: str) -> List[Dict[str, Any]]:
        """Load business rules for a specific report type"""
        return self.validation_rules.get_rules_by_report_type(report_type)

    def execute_rules(self, report_type: str, data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Execute all business rules for a report type against data"""
        rules = self.get_rules(report_type)
        violations = []

        for rule in rules:
            try:
                if self._evaluate_rule(rule, data):
                    violations.append({
                        'rule_id': rule['rule_id'],
                        'rule_name': rule['rule_name'],
                        'severity': rule.get('severity', 'ERROR'),
                        'description': rule.get('description', ''),
                        'validation_type': rule.get('validation_type', 'BUSINESS')
                    })
            except Exception as e:
                print(f"Error executing rule {rule['rule_id']}: {e}")

        return violations

    def _evaluate_rule(self, rule: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """Evaluate a single rule expression against data"""
        rule_expression = rule['rule_expression']
        validation_type = rule.get('validation_type', 'BUSINESS')

        if validation_type == 'RANGE':
            return self._evaluate_range_rule(rule_expression, data)
        elif validation_type == 'COMPARISON':
            return self._evaluate_comparison_rule(rule_expression, data)
        else:
            # Default: treat as Python expression
            return self._evaluate_expression_rule(rule_expression, data)

    def _evaluate_range_rule(self, expression: str, data: Dict[str, Any]) -> bool:
        """Evaluate range rules like 'amount > 10000'"""
        try:
            # Parse expression like "amount > 10000"
            parts = expression.split()
            if len(parts) != 3:
                return False

            field, operator, value = parts
            field_value = data.get(field)

            if field_value is None:
                return False

            # Convert field value to numeric if it's a string
            if isinstance(field_value, str):
                try:
                    if '.' in field_value:
                        field_value = Decimal(field_value)
                    else:
                        field_value = int(field_value)
                except (ValueError, TypeError):
                    # If conversion fails, keep as string
                    pass

            # Convert comparison value to numeric
            try:
                if '.' in value:
                    value = Decimal(value)
                else:
                    value = int(value)
            except (ValueError, TypeError):
                # Keep as string if conversion fails
                pass

            if operator == '>':
                return field_value > value
            elif operator == '<':
                return field_value < value
            elif operator == '>=':
                return field_value >= value
            elif operator == '<=':
                return field_value <= value
            elif operator == '==':
                return field_value == value
            elif operator == '!=':
                return field_value != value

        except Exception as e:
            print(f"Error evaluating range rule: {e}")

        return False

    def _evaluate_comparison_rule(self, expression: str, data: Dict[str, Any]) -> bool:
        """Evaluate field comparison rules like 'total_amount != quantity * unit_price'"""
        try:
            # Create a safe context with converted data
            context = {}
            for field, value in data.items():
                if isinstance(value, str):
                    # Try to convert strings to numbers
                    try:
                        if '.' in value:
                            context[field] = float(value)
                        else:
                            context[field] = int(value)
                    except (ValueError, TypeError):
                        context[field] = value
                else:
                    context[field] = value

            # Add safe functions
            context.update({
                'abs': abs,
                'len': len,
                'str': str,
                'int': int,
                'float': float
            })

            # Evaluate the expression
            return eval(expression, {"__builtins__": {}}, context)
        except Exception as e:
            print(f"Error evaluating comparison rule: {e}")
            return False

    def _evaluate_expression_rule(self, expression: str, data: Dict[str, Any]) -> bool:
        """Evaluate custom Python expressions"""
        try:
            # Create a safe context with converted data
            context = {}
            for field, value in data.items():
                if isinstance(value, str):
                    # Try to convert strings to numbers
                    try:
                        if '.' in value:
                            context[field] = Decimal(value)
                        else:
                            context[field] = int(value)
                    except (ValueError, TypeError):
                        context[field] = value
                else:
                    context[field] = value

            # Add safe functions
            context.update({
                'Decimal': Decimal,
                'abs': abs,
                'len': len,
                'str': str,
                'int': int,
                'float': float
            })

            # Evaluate expression
            return eval(expression, {"__builtins__": {}}, context)
        except Exception as e:
            print(f"Error evaluating expression rule: {e}")
            return False
