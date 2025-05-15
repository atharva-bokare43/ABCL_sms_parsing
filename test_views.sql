-- Monthly Average SIP Investment Per Customer

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
    COALESCE(SUM(s.total_sip_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_sip,
    MAX(investment_date) AS last_sip_investment_date
FROM date_range d
LEFT JOIN sip_monthly s ON d.customer_id = s.customer_id
LEFT JOIN sip_investments si ON d.customer_id = si.customer_id
GROUP BY d.customer_id, d.months_count;


SELECT * FROM vw_customer_emi_summary;

-- Monthly Average Salary Credit Per Customer

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
    COALESCE(SUM(sa.total_salary_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_salary,
    MAX(transaction_date) AS last_salary_date,
    MAX(employer) AS last_employer
FROM date_range d
LEFT JOIN salary_monthly sa ON d.customer_id = sa.customer_id
LEFT JOIN salary_transactions st ON d.customer_id = st.customer_id
GROUP BY d.customer_id, d.months_count;


select * from vw_customer_salary_summary;

--Monthly Average SIP Investment Per Customer
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
    COALESCE(SUM(s.total_sip_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_sip,
    MAX(investment_date) AS last_sip_investment_date
FROM date_range d
LEFT JOIN sip_monthly s ON d.customer_id = s.customer_id
LEFT JOIN sip_investments si ON d.customer_id = si.customer_id
GROUP BY d.customer_id, d.months_count;


select * from vw_customer_sip_summary;



--Monthly Average Insurance Payments Per Customer


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
    COALESCE(SUM(i.total_insurance_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_insurance_payment,
    MAX(transaction_date) AS last_insurance_payment_date,
    MAX(insurance_company) AS last_insurance_company
FROM date_range d
LEFT JOIN insurance_monthly i ON d.customer_id = i.customer_id
LEFT JOIN insurance_payments ip ON d.customer_id = ip.customer_id
GROUP BY d.customer_id, d.months_count;



select * from vw_customer_insurance_summary;









--Average Monthly Debit and Credit Transactions Per Customer


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
    COALESCE(SUM(debit.total_debit_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_debit_amount,
    COALESCE(SUM(credit.total_credit_in_month) / NULLIF(d.months_count, 0), 0) AS avg_monthly_credit_amount,
    MAX(gt.transaction_date) AS last_transaction_date
FROM date_range d
LEFT JOIN debit_monthly debit ON d.customer_id = debit.customer_id
LEFT JOIN credit_monthly credit ON d.customer_id = credit.customer_id
LEFT JOIN general_transactions gt ON d.customer_id = gt.customer_id
GROUP BY d.customer_id, d.months_count;


select * from vw_customer_transactions_summary;







-- Distinct Loan Count Per Customer from EMI Payments
CREATE OR REPLACE VIEW vw_customer_loan_summary AS
SELECT
    customer_id,
    COUNT(DISTINCT loan_reference) AS distinct_loan_count,
    MAX(payment_date) AS last_emi_payment_date
FROM emi_payments
WHERE loan_reference IS NOT NULL AND loan_reference <> ''
GROUP BY customer_id;


select * from vw_customer_loan_summary;
