-- Combined customer insights views

-- 1. EMI Summary
CREATE OR REPLACE VIEW vw_customer_emi_summary AS
WITH date_range AS (
    SELECT customer_id,
           MIN(date_trunc('month', payment_date)) AS first_month,
           MAX(date_trunc('month', payment_date)) AS last_month,
           EXTRACT(MONTH FROM AGE(MAX(payment_date), MIN(payment_date))) + 1 AS months_count
    FROM emi_payments
    GROUP BY customer_id
),
emi_monthly AS (
    SELECT customer_id,
           date_trunc('month', payment_date) AS month,
           SUM(amount) AS total_emi_in_month
    FROM emi_payments
    GROUP BY customer_id, month
)
SELECT
    d.customer_id,
    cu.name AS customer_name,
    COALESCE(SUM(e.total_emi_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_emi,
    MAX(payment_date) AS last_emi_payment_date
FROM date_range d
LEFT JOIN emi_monthly e ON d.customer_id = e.customer_id
LEFT JOIN emi_payments p ON d.customer_id = p.customer_id
JOIN customers cu ON d.customer_id = cu.id
GROUP BY d.customer_id, d.months_count, cu.name;

-- 2. SIP Summary
CREATE OR REPLACE VIEW vw_customer_sip_summary AS
WITH date_range AS (
    SELECT customer_id,
           MIN(date_trunc('month', investment_date)) AS first_month,
           MAX(date_trunc('month', investment_date)) AS last_month,
           EXTRACT(MONTH FROM AGE(MAX(investment_date), MIN(investment_date))) + 1 AS months_count
    FROM sip_investments
    GROUP BY customer_id
),
sip_monthly AS (
    SELECT customer_id,
           date_trunc('month', investment_date) AS month,
           SUM(amount) AS total_sip_in_month
    FROM sip_investments
    GROUP BY customer_id, month
)
SELECT
    d.customer_id,
    cu.name AS customer_name,
    COALESCE(SUM(s.total_sip_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_sip,
    MAX(investment_date) AS last_sip_investment_date
FROM date_range d
LEFT JOIN sip_monthly s ON d.customer_id = s.customer_id
LEFT JOIN sip_investments si ON d.customer_id = si.customer_id
JOIN customers cu ON d.customer_id = cu.id
GROUP BY d.customer_id, d.months_count, cu.name;

-- 3. Salary Summary
CREATE OR REPLACE VIEW vw_customer_salary_summary AS
WITH date_range AS (
    SELECT customer_id,
           MIN(date_trunc('month', transaction_date)) AS first_month,
           MAX(date_trunc('month', transaction_date)) AS last_month,
           EXTRACT(MONTH FROM AGE(MAX(transaction_date), MIN(transaction_date))) + 1 AS months_count
    FROM salary_transactions
    GROUP BY customer_id
),
salary_monthly AS (
    SELECT customer_id,
           date_trunc('month', transaction_date) AS month,
           SUM(amount) AS total_salary_in_month
    FROM salary_transactions
    GROUP BY customer_id, month
)
SELECT
    d.customer_id,
    cu.name AS customer_name,
    COALESCE(SUM(sa.total_salary_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_salary,
    MAX(transaction_date) AS last_salary_date,
    MAX(employer) AS last_employer
FROM date_range d
LEFT JOIN salary_monthly sa ON d.customer_id = sa.customer_id
LEFT JOIN salary_transactions st ON d.customer_id = st.customer_id
JOIN customers cu ON d.customer_id = cu.id
GROUP BY d.customer_id, d.months_count, cu.name;

-- 4. Insurance Summary
CREATE OR REPLACE VIEW vw_customer_insurance_summary AS
WITH date_range AS (
    SELECT customer_id,
           MIN(date_trunc('month', transaction_date)) AS first_month,
           MAX(date_trunc('month', transaction_date)) AS last_month,
           EXTRACT(MONTH FROM AGE(MAX(transaction_date), MIN(transaction_date))) + 1 AS months_count
    FROM insurance_payments
    GROUP BY customer_id
),
insurance_monthly AS (
    SELECT customer_id,
           date_trunc('month', transaction_date) AS month,
           SUM(amount) AS total_insurance_in_month
    FROM insurance_payments
    GROUP BY customer_id, month
)
SELECT
    d.customer_id,
    cu.name AS customer_name,
    COALESCE(SUM(i.total_insurance_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_insurance_payment,
    MAX(transaction_date) AS last_insurance_payment_date,
    MAX(insurance_company) AS last_insurance_company
FROM date_range d
LEFT JOIN insurance_monthly i ON d.customer_id = i.customer_id
LEFT JOIN insurance_payments ip ON d.customer_id = ip.customer_id
JOIN customers cu ON d.customer_id = cu.id
GROUP BY d.customer_id, d.months_count, cu.name;

-- 5. Transactions Summary (Debit & Credit)
CREATE OR REPLACE VIEW vw_customer_transactions_summary AS
WITH date_range AS (
    SELECT customer_id,
           MIN(date_trunc('month', transaction_date)) AS first_month,
           MAX(date_trunc('month', transaction_date)) AS last_month,
           EXTRACT(MONTH FROM AGE(MAX(transaction_date), MIN(transaction_date))) + 1 AS months_count
    FROM general_transactions
    GROUP BY customer_id
),
debit_monthly AS (
    SELECT customer_id,
           date_trunc('month', transaction_date) AS month,
           SUM(amount) AS total_debit_in_month
    FROM general_transactions
    WHERE transaction_type ILIKE '%DEBIT%'
    GROUP BY customer_id, month
),
credit_monthly AS (
    SELECT customer_id,
           date_trunc('month', transaction_date) AS month,
           SUM(amount) AS total_credit_in_month
    FROM general_transactions
    WHERE transaction_type ILIKE '%CREDIT%'
    GROUP BY customer_id, month
)
SELECT
    d.customer_id,
    cu.name AS customer_name,
    COALESCE(SUM(debit.total_debit_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_debit_amount,
    COALESCE(SUM(credit.total_credit_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_credit_amount,
    MAX(gt.transaction_date) AS last_transaction_date
FROM date_range d
LEFT JOIN debit_monthly debit ON d.customer_id = debit.customer_id
LEFT JOIN credit_monthly credit ON d.customer_id = credit.customer_id
LEFT JOIN general_transactions gt ON d.customer_id = gt.customer_id
JOIN customers cu ON d.customer_id = cu.id
GROUP BY d.customer_id, d.months_count, cu.name;

-- 6. Loan Summary
CREATE OR REPLACE VIEW vw_customer_loan_summary AS
SELECT
    customer_id,
    cu.name AS customer_name,
    COUNT(DISTINCT loan_reference) AS distinct_loan_count,
    MAX(payment_date) AS last_emi_payment_date
FROM emi_payments
JOIN customers cu ON emi_payments.customer_id = cu.id
WHERE loan_reference IS NOT NULL AND loan_reference <> ''
GROUP BY customer_id, cu.name;

-- 7. Combined Financial Dashboard
CREATE OR REPLACE VIEW vw_customer_financial_dashboard AS
SELECT
    c.id AS customer_id,
    c.name AS customer_name,
    COALESCE(emi.avg_monthly_emi, 0) AS avg_monthly_emi,
    COALESCE(sip.avg_monthly_sip, 0) AS avg_monthly_sip,
    COALESCE(salary.avg_monthly_salary, 0) AS avg_monthly_salary,
    COALESCE(ins.avg_monthly_insurance_payment, 0) AS avg_monthly_insurance_payment,
    COALESCE(txn.avg_monthly_debit_amount, 0) AS avg_monthly_debit,
    COALESCE(txn.avg_monthly_credit_amount, 0) AS avg_monthly_credit,
    COALESCE(loan.distinct_loan_count, 0) AS loan_count
FROM customers c
LEFT JOIN vw_customer_emi_summary emi ON c.id = emi.customer_id
LEFT JOIN vw_customer_sip_summary sip ON c.id = sip.customer_id
LEFT JOIN vw_customer_salary_summary salary ON c.id = salary.customer_id
LEFT JOIN vw_customer_insurance_summary ins ON c.id = ins.customer_id
LEFT JOIN vw_customer_transactions_summary txn ON c.id = txn.customer_id
LEFT JOIN vw_customer_loan_summary loan ON c.id = loan.customer_id
ORDER BY c.id;

