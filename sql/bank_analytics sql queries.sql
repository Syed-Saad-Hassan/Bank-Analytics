-- To create bank_analytics database

CREATE DATABASE bank_analytics;
USE bank_analytics;

-- for creating loan_raw table

CREATE TABLE loan_raw (
    state_abbr VARCHAR(10),
    account_id VARCHAR(30),
    age_group VARCHAR(20),
    bh_name VARCHAR(100),
    bank_name VARCHAR(100),
    branch_name VARCHAR(100),
    caste VARCHAR(20),
    center_id VARCHAR(20),
    city VARCHAR(100),
    client_id VARCHAR(20),
    client_name VARCHAR(100),
    close_client VARCHAR(10),
    closed_date VARCHAR(20),
    credit_officer VARCHAR(100),
    dob VARCHAR(20),
    disb_by VARCHAR(50),
    disbursement_date VARCHAR(20),
    disb_year VARCHAR(10),
    gender VARCHAR(10),
    home_ownership VARCHAR(50),
    loan_status VARCHAR(50),
    loan_transferdate VARCHAR(20),
    next_meeting VARCHAR(20),
    product_code VARCHAR(20),
    grade VARCHAR(10),
    sub_grade VARCHAR(10),
    product_id VARCHAR(20),
    purpose_category VARCHAR(50),
    region_name VARCHAR(50),
    religion VARCHAR(50),
    verification_status VARCHAR(50),
    state_name VARCHAR(50),
    transfer_logic VARCHAR(50),
    is_delinquent VARCHAR(10),
    is_default VARCHAR(10),
    age_numeric VARCHAR(10),
    delinq_2yrs INT,
    application_type VARCHAR(50),
    loan_amount DECIMAL(12,2),
    funded_amount DECIMAL(12,2),
    funded_amount_inv DECIMAL(12,2),
    term VARCHAR(20),
    int_rate VARCHAR(10),
    total_payment DECIMAL(12,2),
    total_payment_inv DECIMAL(12,2),
    total_principal DECIMAL(12,2),
    total_fees DECIMAL(12,2),
    total_interest DECIMAL(12,2),
    late_fee DECIMAL(12,2),
    recoveries DECIMAL(12,2),
    collection_recovery_fee DECIMAL(12,2)
);


-- for creating loan_clean table

CREATE TABLE loan_clean AS
SELECT
    -- Geography
    state_name,
    state_abbr,
    region_name,
    city,

    -- Bank hierarchy
    bank_name,
    branch_name,

    -- Customer
    account_id,
    gender,
    dob,
    age_group,
    caste,
    religion,
    home_ownership,

    -- Loan details
    product_code,
    product_id,
    application_type,
    purpose_category,
    grade,
    sub_grade,
    loan_status,
    verification_status,

    -- Dates
    disbursement_date,
    
    -- Financials
    loan_amount,
    funded_amount,
    term,
    int_rate,
    total_payment,
    total_principal,
    total_interest,
    total_fees,
    recoveries,
    collection_recovery_fee,

    -- Risk flags
    is_delinquent,
    delinq_2yrs,
    is_default

FROM loan_raw;

-- Fix Dates

ALTER TABLE loan_clean
ADD disb_date DATE;

UPDATE loan_clean
SET disb_date = STR_TO_DATE(disbursement_date, '%d-%m-%Y');


-- Convert Yes/No → 1/0

UPDATE loan_clean
SET is_delinquent = CASE WHEN is_delinquent = 'Yes' THEN 1 ELSE 0 END;

UPDATE loan_clean
SET is_default = CASE WHEN is_default = 'Yes' THEN 1 ELSE 0 END;

-- Remove NULL Loan Amounts

DELETE FROM loan_clean
WHERE loan_amount IS NULL OR loan_amount = 0;

-- WINDOW FUNCTIONS

SELECT
    state_name,
    disb_date,
    loan_amount,
    SUM(loan_amount) OVER (
        PARTITION BY state_name
        ORDER BY disb_date
    ) AS running_total
FROM loan_clean;

-- SUBQUERIES

SELECT *
FROM loan_clean
WHERE loan_amount >
(
    SELECT AVG(loan_amount)
    FROM loan_clean
);


-- STORED PROCEDURE

DELIMITER $$

CREATE PROCEDURE branch_summary()
BEGIN
    SELECT
        branch_name,
        COUNT(*) AS total_loans,
        SUM(loan_amount) AS total_amount,
        SUM(is_default) AS defaults
    FROM loan_clean
    GROUP BY branch_name;
END $$

DELIMITER ;

CALL branch_summary();

-- INDEX

CREATE INDEX idx_state ON loan_clean(state_name);
CREATE INDEX idx_branch ON loan_clean(branch_name);
CREATE INDEX idx_date ON loan_clean(disb_date);

-- TRIGGER

DELIMITER $$

CREATE TRIGGER no_negative_loan
BEFORE INSERT ON loan_clean
FOR EACH ROW
BEGIN
    IF NEW.loan_amount < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Loan amount cannot be negative';
    END IF;
END $$

DELIMITER ;

-- VALUES IN MILLIONS

SELECT 
    state_name,
    CONCAT(ROUND(SUM(loan_amount) / 1000000, 1),
            'M') AS loan_amount_million
FROM
    loan_clean
GROUP BY state_name;

-- KPI 1 :- Total Loan Amount Funded

SELECT 
   concat(ROUND(SUM(loan_amount)/1000000, 2),' M ') AS total_loan_amount_million
FROM loan_clean;


-- KPI 2 :- Total Loans (Count)

SELECT 
    CONCAT(
        ROUND(COUNT(account_id)/1000, 1),
        ' K'
    ) AS total_loans_count
FROM loan_clean;

-- KPI :- 3 Total Collection

SELECT 
    concat(round(SUM(total_payment)/1000000, 2), ' M')
    AS total_collection_million
FROM loan_clean;

-- KPI :- 4 Total Interest

SELECT 
    concat(round(SUM(total_interest)/1000000, 2),' M ') AS total_interest_million
FROM loan_clean;

-- KPI :- 5 Branch-Wise Performance

SELECT
    branch_name,
    concat(round(SUM(total_interest)/1000000, 2), ' M') AS interest_million,
    concat(ROUND(SUM(total_fees)/1000000, 2), ' M') AS fees_million,
    concat(ROUND(SUM(loan_amount)/1000000, 2), ' M') AS total_loan_million
FROM loan_clean
GROUP BY branch_name
ORDER BY total_loan_million DESC;


-- KPI :- 6 State-Wise Loan

SELECT
    state_name,
    concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY state_name
ORDER BY loan_amount_million DESC;

-- KPI :- 7 Religion-Wise Loan

SELECT
    religion,
    concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY religion;

-- KPI :- 8 Product Group-Wise Loan

SELECT
    purpose_category,
     concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY purpose_category;

-- KPI :- 9 Disbursement Trend (Time Series)

SELECT
    YEAR(disb_date) AS disb_year,
    concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY YEAR(disb_date)
ORDER BY disb_year;


-- KPI :- 10 Grade-Wise Loan (Risk Analysis)


SELECT
    grade,
     concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY grade
ORDER BY grade;

-- KPI :- 11 Loan Status-Wise Loan

select
    loan_status,
    COUNT(*) AS total_loans,
    concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY loan_status;

--  KPI :- 12 Age Group-Wise Loan


SELECT
    age_group,
    concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY age_group;

--  KPI :- 13 Loan Maturity (by Term)

SELECT
    term,
    COUNT(*) AS total_loans,
    concat(round(SUM(loan_amount)/1000000, 2), ' M') AS loan_amount_million
FROM loan_clean
GROUP BY term;

















