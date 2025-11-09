-- Redshift table schemas for CleanELT
-- Usage: Execute this script in your Redshift cluster

-- Sales Reports Table
CREATE TABLE IF NOT EXISTS sales_reports (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL,
    transaction_date DATE NOT NULL,
    source_file VARCHAR(255),
    processed_at TIMESTAMP DEFAULT GETDATE(),
    UNIQUE(transaction_id, source_file)
);

-- Inventory Reports Table  
CREATE TABLE IF NOT EXISTS inventory_reports (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_level INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    source_file VARCHAR(255),
    processed_at TIMESTAMP DEFAULT GETDATE(),
    UNIQUE(product_id, source_file, last_updated)
);

-- Expense Reports Table
CREATE TABLE IF NOT EXISTS expense_reports (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    expense_id VARCHAR(50) NOT NULL,
    employee_id VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    expense_date DATE NOT NULL,
    description VARCHAR(500),
    source_file VARCHAR(255),
    processed_at TIMESTAMP DEFAULT GETDATE(),
    UNIQUE(expense_id, source_file)
);

-- Data Quality Audit Table
CREATE TABLE IF NOT EXISTS data_quality_audit (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    total_rows INTEGER NOT NULL,
    valid_rows INTEGER NOT NULL,
    invalid_rows INTEGER NOT NULL,
    validation_errors TEXT,
    processed_at TIMESTAMP DEFAULT GETDATE()
);
