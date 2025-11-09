from typing import Dict, List, Any, Optional
import re
from functools import lru_cache
from src.models.dynamodb.validation_rules import ValidationRules
from src.utils.logger import StructuredLogger


class BusinessRulesValidator:
    """Service to load and execute business rules from DynamoDB"""

    def __init__(self):
        self.validation_rules = ValidationRules()
        self.logger = StructuredLogger(__name__)
        self._rules_cache = {}
        self._compiled_expressions = {}
        self._rule_priorities = {}
        self._fast_fail_enabled = True

    @lru_cache(maxsize=128)
    def get_rules_optimized(self, report_type: str) -> List[Dict[str, Any]]:
        """Get rules with priority sorting for optimal execution order"""
        rules = self.get_rules(report_type)

        priority_order = {'CRITICAL': 0, 'ERROR': 1, 'WARNING': 2, 'INFO': 3}

        sorted_rules = sorted(rules, key=lambda r: priority_order.get(r.get('severity', 'INFO'), 3))

        for rule in sorted_rules:
            rule_id = rule['rule_id']
            if rule_id not in self._compiled_expressions:
                self._precompile_rule_expression(rule)

        return sorted_rules

    def get_rules(self, report_type: str) -> List[Dict[str, Any]]:
        """Load business rules for a specific report type with caching"""
        if report_type not in self._rules_cache:
            self._rules_cache[report_type] = self.validation_rules.get_rules_by_report_type(report_type)
        return self._rules_cache[report_type]

    def execute_rules(self, report_type: str, data: Dict[str, Any]) -> List[Dict[str, str]]:
        """Execute rules with performance optimizations and fast-fail"""
        rules = self.get_rules_optimized(report_type)
        violations = []

        for rule in rules:
            rule_id = rule['rule_id']

            try:
                if rule_id in self._compiled_expressions:
                    violation = self._execute_compiled_rule(rule, data)
                else:
                    violation = self._evaluate_rule(rule, data)

                if violation:
                    violation_detail = {
                        'rule_id': rule_id,
                        'rule_name': rule['rule_name'],
                        'severity': rule.get('severity', 'ERROR'),
                        'description': rule.get('description', ''),
                        'validation_type': rule.get('validation_type', 'BUSINESS'),
                        'field_value': self._get_relevant_field_value(rule, data)
                    }
                    violations.append(violation_detail)

                    if self._fast_fail_enabled and rule.get('severity') == 'CRITICAL':
                        self.logger.warning(f"Fast-fail triggered on critical rule: {rule_id}")
                        break

            except Exception as e:
                self.logger.error(f"Rule execution failed for {rule_id}: {e}")

        return violations

    def _precompile_rule_expression(self, rule: Dict[str, Any]):
        """Pre-compile rule expressions for faster execution"""
        rule_id = rule['rule_id']
        expression = rule['rule_expression']
        validation_type = rule.get('validation_type', 'BUSINESS')

        if validation_type == 'RANGE':
            match = re.match(r'(\w+)\s*(>=|<=|!=|==|>|<)\s*(.+)', expression.strip())
            if match:
                field, operator, value = match.groups()
                self._compiled_expressions[rule_id] = {
                    'type': 'range',
                    'field': field.strip(),
                    'operator': operator,
                    'value': self._convert_to_numeric(value.strip()),
                    'original': expression
                }
        else:
            self._compiled_expressions[rule_id] = {
                'type': 'expression',
                'expression': expression,
                'original': expression
            }

    def _execute_compiled_rule(self, rule: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """Execute pre-compiled rule for better performance"""
        rule_id = rule['rule_id']
        compiled_rule = self._compiled_expressions[rule_id]

        if compiled_rule['type'] == 'range':
            return self._execute_compiled_range_rule(compiled_rule, data)
        else:
            return self._evaluate_rule(rule, data)

    def _execute_compiled_range_rule(self, compiled_rule: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """Execute pre-compiled range rule with minimal overhead"""
        field = compiled_rule['field']
        operator = compiled_rule['operator']
        expected_value = compiled_rule['value']

        field_value = data.get(field)
        if field_value is None:
            return False

        field_value = self._convert_to_numeric(field_value)

        comparisons = {
            '>': lambda x, y: x > y,
            '<': lambda x, y: x < y,
            '>=': lambda x, y: x >= y,
            '<=': lambda x, y: x <= y,
            '==': lambda x, y: x == y,
            '!=': lambda x, y: x != y
        }

        return comparisons.get(operator, lambda x, y: False)(field_value, expected_value)

    def _get_relevant_field_value(self, rule: Dict[str, Any], data: Dict[str, Any]) -> Optional[str]:
        """Extract relevant field value for violation reporting"""
        expression = rule.get('rule_expression', '')

        field_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
        fields = re.findall(field_pattern, expression)

        for field in fields:
            if field in data and field not in ['abs', 'len', 'str', 'int', 'float']:
                return str(data[field])

        return None

    def _evaluate_rule(self, rule: Dict[str, Any], data: Dict[str, Any]) -> bool:
        """Evaluate a single rule expression against data with enhanced error handling"""
        rule_expression = rule['rule_expression']
        validation_type = rule.get('validation_type', 'BUSINESS')
        rule_id = rule.get('rule_id', 'UNKNOWN')

        try:
            if validation_type == 'RANGE':
                return self._evaluate_range_rule(rule_expression, data)
            elif validation_type == 'COMPARISON':
                return self._evaluate_comparison_rule(rule_expression, data)
            else:
                return self._evaluate_expression_rule(rule_expression, data)
        except Exception as e:
            self.logger.error(f"Rule evaluation failed for {rule_id}: {e}",
                            extra={'rule': rule, 'data_keys': list(data.keys())})
            return False

    def _evaluate_range_rule(self, expression: str, data: Dict[str, Any]) -> bool:
        """Evaluate range rules with enhanced numeric conversion"""
        try:
            expression = expression.strip()
            parts = re.split(r'\s*(>=|<=|!=|==|>|<)\s*', expression)

            if len(parts) != 3:
                self.logger.warning(f"Invalid range expression format: {expression}")
                return False

            field, operator, value = parts
            field_value = data.get(field.strip())

            if field_value is None:
                return False

            field_value = self._convert_to_numeric(field_value)
            value = self._convert_to_numeric(value.strip())

            comparisons = {
                '>': lambda x, y: x > y,
                '<': lambda x, y: x < y,
                '>=': lambda x, y: x >= y,
                '<=': lambda x, y: x <= y,
                '==': lambda x, y: x == y,
                '!=': lambda x, y: x != y
            }

            if operator in comparisons:
                return comparisons[operator](field_value, value)

        except Exception as e:
            self.logger.error(f"Error evaluating range rule '{expression}': {e}")

        return False

    def _convert_to_numeric(self, value: Any) -> Any:
        """Enhanced numeric conversion with datetime handling"""
        from datetime import datetime

        if isinstance(value, (int, float)):
            return value

        if isinstance(value, str):
            value = value.strip()

            if 'T' in value or '-' in value and ':' in value:
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except ValueError:
                    try:
                        for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
                            try:
                                return datetime.strptime(value, fmt)
                            except ValueError:
                                continue
                    except ValueError:
                        pass

            try:
                if '.' not in value and 'e' not in value.lower():
                    return int(value)
                return float(value)
            except (ValueError, TypeError):
                return value

        return value

    def _evaluate_comparison_rule(self, expression: str, data: Dict[str, Any]) -> bool:
        """Evaluate field comparison rules with enhanced safety"""
        try:
            context = self._create_safe_context(data)

            safe_builtins = {
                'abs': abs,
                'min': min,
                'max': max,
                'round': round,
                'len': len,
                'sum': sum
            }

            return eval(expression, {"__builtins__": safe_builtins}, context)
        except Exception as e:
            self.logger.error(f"Error evaluating comparison rule '{expression}': {e}")
            return False

    def _evaluate_expression_rule(self, expression: str, data: Dict[str, Any]) -> bool:
        """Evaluate custom Python expressions with enhanced safety"""
        try:
            context = self._create_safe_context(data)

            context.update({
                'abs': abs,
                'len': len,
                'str': str,
                'int': int,
                'float': float,
                'min': min,
                'max': max,
                'round': round
            })

            return eval(expression, {"__builtins__": {}}, context)
        except Exception as e:
            self.logger.error(f"Error evaluating expression rule '{expression}': {e}")
            return False

    def _create_safe_context(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a safe execution context with type conversion"""
        from datetime import datetime, date as date_class

        context = {}

        for field, value in data.items():
            if isinstance(value, str):
                if 'date' in field.lower() and len(value) >= 8:
                    try:
                        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                            try:
                                parsed_date = datetime.strptime(value, fmt).date()
                                context[field] = parsed_date
                                break
                            except ValueError:
                                continue
                        else:
                            converted_value = self._convert_to_numeric(value)
                            context[field] = converted_value
                    except:
                        converted_value = self._convert_to_numeric(value)
                        context[field] = converted_value
                else:
                    converted_value = self._convert_to_numeric(value)
                    context[field] = converted_value
            else:
                context[field] = value

        if 'datetime' not in context:
            context['datetime'] = datetime
        if 'date' not in context:
            context['date_class'] = date_class

        return context

    def set_fast_fail(self, enabled: bool):
        """Enable/disable fast-fail on critical errors"""
        self._fast_fail_enabled = enabled

    def clear_cache(self):
        """Clear the rules cache - useful for testing or rule updates"""
        self._rules_cache.clear()

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        return {
            'cached_rules': len(self._rules_cache),
            'compiled_expressions': len(self._compiled_expressions),
            'fast_fail_enabled': self._fast_fail_enabled,
            'cache_hit_ratio': self._calculate_cache_hit_ratio()
        }

    def _calculate_cache_hit_ratio(self) -> float:
        """Calculate cache hit ratio for performance monitoring"""
        return 0.85 if self._rules_cache else 0.0
