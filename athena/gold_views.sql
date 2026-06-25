-- Gold layer views for ClinicalFlow
-- Access controlled via AWS Lake Formation column-level grants
-- Production enforcement: analyst IAM users have zero direct S3/Glue permissions,
-- access exclusively through Lake Formation grants — restricts columns per role.

-- View: vw_patient_demographics
-- Granted to: analyst role (id, gender, race, ethnicity, state, zip, birth_year_bucket)
-- Excludes: income, healthcare_expenses, healthcare_coverage
CREATE OR REPLACE VIEW clinicalflow_silver.vw_patient_demographics AS
SELECT
    id,
    gender,
    race,
    ethnicity,
    state,
    zip,
    birth_year_bucket
FROM clinicalflow_silver.patients;

-- View: vw_patient_financials
-- Granted to: finance role only
-- Contains income and healthcare cost columns
CREATE OR REPLACE VIEW clinicalflow_silver.vw_patient_financials AS
SELECT
    id,
    income,
    healthcare_expenses,
    healthcare_coverage
FROM clinicalflow_silver.patients;
