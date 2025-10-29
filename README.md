# CleanELT

A serverless data quality validation platform for ETL pipelines, ensuring data integrity and compliance through automated validation workflows.

## Project Summary

CleanELT is a comprehensive data quality checker designed for modern ETL (Extract, Load, Transform) pipelines. It provides real-time validation of data files as they arrive in your data lake, ensuring data quality standards are met before downstream processing begins.

**Key Capabilities:**
- **Automated Validation**: Validates data files automatically upon S3 upload
- **Dual-Layer Validation**: Combines structural field validation with configurable business rules
- **Multi-Format Support**: Handles CSV and JSON data formats seamlessly
- **Scalable Architecture**: Serverless design scales automatically with data volume
- **Comprehensive Monitoring**: Detailed logging and metrics for data quality insights
- **Cost-Effective**: Pay-per-use serverless model with optimized resource allocation

**Use Cases:**
- Data lake ingestion quality gates
- ETL pipeline data validation checkpoints
- Regulatory compliance data verification
- Data warehouse pre-processing validation
- Real-time data quality monitoring

## Architecture

**Pipeline Flow**: S3 → SQS → Lambda → DynamoDB

- **S3**: Stores ETL reports (Sales, Inventory, Expense) with event notifications
- **SQS**: Queues file processing events for reliable processing
- **Lambda**: Processes files with dual-layer validation (field + business rules)
- **DynamoDB**: Stores validation results, business rules, processing logs, and error details

## Features

- **Multi-format Support**: CSV and JSON file processing
- **Dual Validation**: Pydantic field validation + DynamoDB business rules
- **Stage-based Deployment**: {stage}, uat, stg, prd environments
- **Structured Logging**: Contextual logging with request tracking
- **Error Handling**: Comprehensive error capture and storage
- **Resource Tagging**: Automated cost tracking and resource management

## Project Structure

```
├── src/
│   ├── handlers/           # Lambda entry points
│   ├── processors/         # Report-specific processors
│   ├── services/           # Business logic and AWS service wrappers
│   ├── models/             # Pydantic models and enums
│   ├── validators/         # Data validation logic
│   └── utils/              # Shared utilities
├── yaml/                   # Modular serverless configuration
├── scripts/                # Setup and deployment scripts
├── mocks/                  # Test data and events
└── tests/                  # Unit and integration tests
```

## Prerequisites

- Python 3.11+
- Node.js 18+ (for Serverless Framework)
- AWS CLI configured
- Docker (for cross-platform package compilation)

## Setup

### 1. Install Dependencies

```bash
# Install Serverless Framework
npm install -g serverless

# Install Python dependencies
pip install -r requirements.txt

# Install Serverless plugins
npm install
```

### 2. Configure AWS Profile

```bash
# Configure AWS profile for your target environment
aws configure --profile {aws-profile}
```

### 3. Create IAM Execution Role

```bash
# Create Lambda execution role with required permissions
./scripts/create-iam-role.sh {stage} {aws-profile}
```

### 4. Setup Resource Tagging

```bash
# Configure stack tags in SSM Parameter Store
./scripts/setup-tags.sh {stage} {aws-profile}
```

### 5. Populate Business Rules

```bash
# Load validation rules into DynamoDB
python scripts/populate_business_rules.py --stage {stage} --profile {aws-profile}
```

## Deployment

### Deploy to Development

```bash
sls deploy --stage {stage} --aws-profile {aws-profile}
```

### Deploy to Other Stages

```bash
sls deploy --stage uat --aws-profile uat-admin-profile
sls deploy --stage stg --aws-profile stg-admin-profile
sls deploy --stage prd --aws-profile prd-admin-profile
```

## Testing

### Local Testing

```bash
# Test with mock SQS events
sls invoke local --function ReportProcessorHandler --path ./mocks/events/sqs-sales-event.json --aws-profile {aws-profile}
sls invoke local --function ReportProcessorHandler --path ./mocks/events/sqs-inventory-event.json --aws-profile {aws-profile}
sls invoke local --function ReportProcessorHandler --path ./mocks/events/sqs-expense-event.json --aws-profile {aws-profile}
```

### Remote Testing

```bash
# Test deployed Lambda function
sls invoke --function ReportProcessorHandler --path ./mocks/events/sqs-sales-event.json --stage {stage} --aws-profile {aws-profile}
```

### Upload Test Files

```bash
# Upload test files to trigger processing
aws s3 cp mocks/data/sales_report.csv s3://{stage}-etl-data-quality-checker-bucket/Reports/Sales/ --profile {aws-profile}
aws s3 cp mocks/data/inventory_report.csv s3://{stage}-etl-data-quality-checker-bucket/Reports/Inventory/ --profile {aws-profile}
aws s3 cp mocks/data/expense_report.json s3://{stage}-etl-data-quality-checker-bucket/Reports/Expense/ --profile {aws-profile}
```

## Configuration

### Environment Variables

- `STAGE`: Deployment stage ({stage}/uat/stg/prd)

### SSM Parameters

The application uses SSM Parameter Store for configuration:

- `/{stage}/etl-data-quality-checker/EXECUTION_ROLE_ARN`: Lambda execution role
- `/{stage}/etl-data-quality-checker/DOCKER_IMAGE`: Docker image for package compilation
- `/{stage}/etl-data-quality-checker/OWNER`: Resource owner tag
- `/{stage}/etl-data-quality-checker/COST_CENTER`: Cost center tag
- `/{stage}/etl-data-quality-checker/CREATED_DATE`: Creation date tag

### Business Rules

Validation rules are stored in DynamoDB `validation-rules` table:

- **RANGE**: Numeric range validation
- **COMPARISON**: Field comparison validation
- **BUSINESS**: Custom business logic validation

## Supported Report Types

### Sales Reports
- **Path**: `Reports/Sales/`
- **Fields**: transaction_id, product_id, quantity, unit_price, total_amount, transaction_date
- **Formats**: CSV, JSON

### Inventory Reports
- **Path**: `Reports/Inventory/`
- **Fields**: product_id, product_name, category, quantity_on_hand, reorder_level, last_updated
- **Formats**: CSV, JSON

### Expense Reports
- **Path**: `Reports/Expense/`
- **Fields**: expense_id, employee_id, category, amount, expense_date, description
- **Formats**: CSV, JSON

## Monitoring

### CloudWatch Logs

- Lambda execution logs with structured logging
- Error tracking and debugging information
- Performance metrics and processing statistics

### DynamoDB Tables

- `validation-results`: Detailed validation results per file/row
- `validation-rules`: Business validation rules
- `file-processing-log`: Processing status and metrics
- `error-details`: Error details and stack traces

## Cost Optimization

- **Lambda**: Pay-per-execution with 512MB memory allocation
- **DynamoDB**: On-demand billing for variable workloads
- **S3**: Standard storage with lifecycle policies
- **SQS**: Pay-per-message with dead letter queue

## Security

- **IAM**: Least privilege access with specific resource permissions
- **VPC**: Optional VPC deployment for network isolation
- **Encryption**: Server-side encryption for S3 and DynamoDB
- **Secrets**: SSM Parameter Store for sensitive configuration

## Development

### Adding New Report Types

1. Create processor in `src/processors/`
2. Add validator in `src/validators/`
3. Create Pydantic model in `src/models/reports/`
4. Update `ReportType` enum in `src/models/enums.py`
5. Add S3 path configuration in serverless YAML

### Local Development

```bash
# Install development dependencies
pip install -r requirements-{stage}.txt

# Run tests
pytest tests/

# Format code
black src/
isort src/
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure Python path includes `src/` directory
2. **Permission Errors**: Verify IAM role has required permissions
3. **Package Errors**: Use Docker for cross-platform package compilation
4. **Region Errors**: Ensure AWS CLI and serverless.yml use same region

### Debug Commands

```bash
# Check CloudWatch logs
sls logs --function ReportProcessorHandler --stage {stage} --aws-profile {aws-profile}

# Validate serverless configuration
sls print --stage {stage} --aws-profile {aws-profile}

# Check DynamoDB tables
aws dynamodb list-tables --profile {aws-profile} --region ap-southeast-1
```
