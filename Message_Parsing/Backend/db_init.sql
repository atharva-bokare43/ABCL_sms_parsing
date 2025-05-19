
-- customers table
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    phone_number VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- salary_transactions table
CREATE TABLE IF NOT EXISTS salary_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    transaction_date DATE,
    transaction_id VARCHAR(100),
    employer VARCHAR(200),
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_salary_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- emi_payments table
CREATE TABLE IF NOT EXISTS emi_payments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    payment_date DATE,
    loan_reference VARCHAR(100),
    loan_type VARCHAR(100),
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_emi_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- credit_card_transactions table
CREATE TABLE IF NOT EXISTS credit_card_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    bank_name VARCHAR(100),
    card_number VARCHAR(50),
    amount DECIMAL(12, 2),
    merchant VARCHAR(200),
    transaction_date DATE,
    authorization_code VARCHAR(50),
    available_balance DECIMAL(12, 2),
    total_outstanding DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_cc_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- sip_investments table
CREATE TABLE IF NOT EXISTS sip_investments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    fund_name VARCHAR(200),
    folio_number VARCHAR(100),
    amount DECIMAL(12, 2),
    investment_date DATE,
    nav_value DECIMAL(12, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_sip_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- general_transactions table
CREATE TABLE IF NOT EXISTS general_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    transaction_type VARCHAR(50),
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    amount DECIMAL(12, 2),
    transaction_date DATE,
    available_balance DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_general_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);

-- insurance_payments table
CREATE TABLE IF NOT EXISTS insurance_payments (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    policy_number VARCHAR(100),
    insurance_company VARCHAR(100),
    insurance_type VARCHAR(100),
    amount DECIMAL(12, 2),
    transaction_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_insurance_customer FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS raw_messages (
    id SERIAL PRIMARY KEY,
    message_text TEXT,
    message_type VARCHAR(100),
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


