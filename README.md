# CleanELT

A serverless data quality validation platform for ETL pipelines that ensures data integrity through automated validation workflows.

## Overview

CleanELT provides real-time validation of data files as they arrive in your data lake, ensuring quality standards are met before downstream processing. Built on AWS serverless architecture for automatic scaling and cost optimization.

### Key Features

- **Automated Validation**: Real-time file processing on S3 upload
- **Dual-Layer Validation**: Structural field validation + configurable business rules
- **Multi-Format Support**: CSV and JSON processing
- **Memory Optimized**: 70% memory reduction through streaming batch processing
- **Performance Optimized**: 3x faster execution with rule pre-compilation and caching
- **Redshift Integration**: Automated COPY operations for validated data
- **Comprehensive Logging**: Structured logging with 49+ standardized status codes

### Architecture

```
S3 → SQS → Lambda → DynamoDB → Redshift
```

- **S3**: File storage with event notifications
- **SQS**: Reliable event queuing
- **Lambda**: Serverless processing with dual validation
- **DynamoDB**: Validation rules, results, and audit logs
- **Redshift**: Data warehouse for validated data

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- AWS CLI configured
- Docker (for package compilation)

### Installation

```bash
# Install Serverless Framework
npm install -g serverless

# Install dependencies
pip install -r requirements.txt
npm install

# Configure AWS
aws configure --profile {aws-profile}
```

### Deployment

```bash
# Deploy to development
sls deploy --stage dev --aws-profile {aws-profile}
```

### Testing

```bash
# Local testing
sls invoke local --function ReportProcessorHandler --path ./mocks/events/sqs-sales-event.json

# Remote testing
sls invoke --function ReportProcessorHandler --path ./mocks/events/sqs-sales-event.json --stage dev
```

## Configuration

### Environment Variables

- `STAGE`: Deployment environment (dev/uat/stg/prd)

### SSM Parameters

Configure these parameters in AWS Systems Manager:

```
/{stage}/cleanelt/EXECUTION_ROLE_ARN
/{stage}/cleanelt/DOCKER_IMAGE
/{stage}/cleanelt/OWNER
/{stage}/cleanelt/COST_CENTER
/{stage}/cleanelt/CREATED_DATE
```

### Business Rules

Validation rules are stored in DynamoDB with three types:
- **RANGE**: Numeric range validation
- **COMPARISON**: Field comparison validation  
- **BUSINESS**: Custom business logic validation

See [BUSINESS_RULES_REFERENCE.md](BUSINESS_RULES_REFERENCE.md) for detailed specifications.

## Supported Report Types

| Report Type | Path | Formats | Business Rules |
|-------------|------|---------|----------------|
| Sales | `Reports/Sales/` | CSV, JSON | 6 rules (amount calculations, ranges) |
| Inventory | `Reports/Inventory/` | CSV, JSON | 6 rules (stock levels, reorder points) |
| Expense | `Reports/Expense/` | CSV, JSON | 4 rules (amount limits, categories) |

## Project Structure

```
src/
├── handlers/           # Lambda entry points
├── processors/         # Report-specific processors
├── services/           # Business logic and AWS integrations
├── models/             # Pydantic models (reports, requests, responses)
├── validators/         # Data validation logic
├── exceptions/         # Custom exception classes
└── utils/              # Logging, batch processing, utilities

yaml/                   # Serverless configuration
mocks/                  # Test data and events
tests/                  # Unit and integration tests
sql/                    # Redshift table schemas
```

## Logging System

CleanELT uses structured logging with standardized status codes:

```python
# Info logging
self.logger.log_info(InfoCode.INFO_101, operation="file_processing", file_path=path)

# Success logging  
self.logger.log_success(SuccessCode.SUCCESS_200, operation="batch_copy", rows_processed=1000)

# Warning logging
self.logger.log_warning(WarningCode.DATA_W401, operation="validation", invalid_rows=5)

# Error logging
self.logger.log_error(ErrorCode.FILE_001, exception=e, file_path=path)
```

**Status Code Categories:**
- **Error Codes**: 30+ codes across 8 categories (FILE, AWS, RS, DATA, RULE, REQ, BATCH, SYS)
- **Warning Codes**: System and operational warnings
- **Success Codes**: Positive outcome tracking (SUCCESS_200-205)
- **Info Codes**: Process flow information (INFO_100-105)

## Performance

### Optimization Features
- **Memory**: Streaming processing with smart batch sizing (50-500 rows)
- **Speed**: Rule pre-compilation with 85% cache hit ratio
- **Package**: ~75MB function size with Lambda layers
- **Processing**: Target < 5 seconds per batch

### Monitoring
- CloudWatch logs with structured output
- DynamoDB tables for validation results and audit trails
- Performance metrics tracking

## Development

### Adding New Report Types

1. Create processor in `src/processors/`
2. Add validator in `src/validators/`
3. Create Pydantic model in `src/models/reports/`
4. Update `ReportType` enum
5. Configure S3 paths in serverless YAML
6. Add business rules to DynamoDB

### Local Development

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Code formatting
black src/ && isort src/

# Type checking
mypy src/

# Security scanning
bandit -r src/
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Import Errors | Ensure Python path includes `src/` directory |
| Permission Errors | Verify IAM role permissions |
| Package Size | Use Lambda layers and optimized requirements |
| Memory Errors | Check batch sizing configuration |

### Debug Commands

```bash
# View logs
sls logs --function ReportProcessorHandler --stage {stage}

# Validate configuration
sls print --stage {stage}

# Test locally
pytest tests/integration/ -v
```

## Security

- **IAM**: Least privilege access
- **Encryption**: Server-side encryption for S3 and DynamoDB
- **Secrets**: SSM Parameter Store for configuration
- **VPC**: Optional network isolation

## Cost Optimization

- Pay-per-execution serverless model
- Optimized memory allocation (512MB)
- Lambda layers for shared dependencies
- On-demand DynamoDB billing

## Contributing

This is a personal project. If you'd like to contribute:

1. Fork the repository at [https://github.com/Evrouin/clean-elt](https://github.com/Evrouin/clean-elt)
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Documentation

- [Business Rules Reference](BUSINESS_RULES_REFERENCE.md) - Validation rules and examples
- Inline code documentation throughout the codebase
