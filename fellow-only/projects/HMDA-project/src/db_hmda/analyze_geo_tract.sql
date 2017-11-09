DROP TABLE IF EXISTS Tract_Level;
CREATE TABLE Tract_Level AS
    SELECT State_Code, County_Code, Census_Tract_Number, Loan_Type, Action_Type, COUNT(1) as NUM_BORROWERS, sum(Loan_Amount) as TOTAL_AMOUNT
    FROM HMDA_LAR
    WHERE Loan_Purpose = 1 AND Property_Type = 1
    GROUP BY State_Code, County_Code, Census_Tract_Number, Loan_Type, Action_Type;

DROP TABLE IF EXISTS FHA_by_tract;
CREATE TABLE FHA_by_tract AS
    SELECT State_Code, County_Code, Census_Tract_Number, SUM(NUM_BORROWERS) AS NUM_ALL, SUM(NUM_BORROWERS * (Loan_Type = 2)::int) AS NUM_FHA, SUM(TOTAL_AMOUNT) as AMT_ALL, SUM(TOTAL_AMOUNT * (Loan_Type = 2)::int) as AMT_FHA
    FROM Tract_Level
    WHERE ACTION_TYPE = 1
    GROUP BY State_Code, County_Code, Census_Tract_Number;

ALTER TABLE FHA_by_tract ADD PCT_NUM_FHA REAL;
UPDATE FHA_by_tract
    SET PCT_NUM_FHA = NUM_FHA*100.0/NUM_ALL;

ALTER TABLE FHA_by_tract ADD PCT_AMT_FHA REAL;
UPDATE FHA_by_tract
    SET PCT_AMT_FHA = AMT_FHA*100.0/AMT_ALL;

\COPY ( SELECT State_Code, County_Code, Census_Tract_Number, NUM_ALL, NUM_FHA, PCT_NUM_FHA, AMT_ALL, AMT_FHA, PCT_AMT_FHA  FROM FHA_by_tract ORDER BY PCT_AMT_FHA DESC ) TO 'data/fha_by_tract.csv' WITH CSV
