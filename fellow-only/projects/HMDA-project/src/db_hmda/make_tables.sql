DROP TABLE IF EXISTS hmda_lar;
CREATE TABLE hmda_lar (
    As_of_Year INTEGER,
    Respondent_ID TEXT NOT NULL,
    Agency_Code INTEGER,
    Loan_Type INTEGER,
    Property_Type INTEGER,
    Loan_Purpose INTEGER,
    Occupancy TEXT ,
    Loan_Amount INTEGER,
    Preapproval INTEGER,
    Action_Type INTEGER,
    MSA_MD TEXT,
    State_Code TEXT,
    County_Code TEXT,
    Census_Tract_Number TEXT,
    Applicant_Ethnicity TEXT,
    Co_Applicant_Ethnicity TEXT,
    Applicant_Race_1 TEXT,
    Applicant_Race_2 TEXT,
    Applicant_Race_3 TEXT,
    Applicant_Race_4 TEXT,
    Applicant_Race_5 TEXT,
    Co_Applicant_Race_1 TEXT,
    Co_Applicant_Race_2 TEXT,
    Co_Applicant_Race_3 TEXT,
    Co_Applicant_Race_4 TEXT,
    Co_Applicant_Race_5 TEXT,
    Applicant_Sex TEXT,
    Co_Applicant_Sex TEXT,
    Applicant_Income TEXT,
    Purchaser_Type TEXT,
    Denial_Reason_1 TEXT,
    Denial_Reason_2 TEXT,
    Denial_Reason_3 TEXT,
    Rate_Spread TEXT,
    HOEPA_Status TEXT,
    Lien_Status TEXT,
    Edit_Status TEXT,
    Sequence_Number INTEGER NOT NULL,
    Population INTEGER,
    Minority_Population_Pct REAL,
    HUD_Median_Family_Income INTEGER,
    Tract_to_MSA_MD_Income_Pct REAL,
    Number_of_Owner_occupied_units INTEGER,
    Number_of_1_to_4_Family_units INTEGER,
    Application_Date_Indicator TEXT
);

DROP TABLE IF EXISTS pmic_lar;
CREATE TABLE pmic_lar AS SELECT * FROM hmda_lar LIMIT 0;

DROP TABLE IF EXISTS agencies;
CREATE TABLE agencies (Agency_Code INTEGER PRIMARY KEY, AGENCY_NAME TEXT);
\COPY agencies FROM 'dicts/agencies.csv' WITH DELIMITER ',' CSV;

DROP TABLE IF EXISTS loan_types;
CREATE TABLE loan_types (Loan_Type INTEGER PRIMARY KEY, Loan_type_name TEXT, Loan_type_description TEXT );
\COPY loan_types FROM 'dicts/loan-types.csv'  WITH DELIMITER '|' CSV;
