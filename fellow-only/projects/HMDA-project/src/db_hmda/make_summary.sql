---- Summary by Tracts ----

DROP TABLE IF EXISTS tracts_by_mktshare;
CREATE TABLE tracts_by_mktshare (mktshare REAL PRIMARY KEY, percent_of_tracts REAL, percent_of_borrowers REAL, percent_of_amount REAL);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (30);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (35);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (40);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (45);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (50);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (55);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (60);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (65);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (70);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (75);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (80);
INSERT INTO tracts_by_mktshare (mktshare) VALUES (85);



UPDATE tracts_by_mktshare SET 
    percent_of_tracts = (SELECT sum((pct_amt_fha >= tracts_by_mktshare.mktshare)::int)*100.0/count(1) FROM FHA_by_TRACT), 
    percent_of_borrowers = (SELECT sum(num_all * (pct_amt_fha >= tracts_by_mktshare.mktshare)::int)*100.0/sum(num_all) FROM FHA_BY_TRACT),
    percent_of_amount = (SELECT sum(amt_all * (pct_amt_fha >= tracts_by_mktshare.mktshare)::int)*100.0/sum(amt_all) FROM FHA_BY_TRACT);

\COPY (SELECT * FROM tracts_by_mktshare) TO 'data/summary_tracts_by_mktshare.csv' WITH CSV;


---- Summary by MSA ----

DROP TABLE IF EXISTS MSAs_by_mktshare;
CREATE TABLE MSAs_by_mktshare (mktshare REAL PRIMARY KEY, percent_of_MSAs REAL, percent_of_borrowers REAL, percent_of_amount REAL);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (30);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (35);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (40);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (45);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (50);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (55);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (60);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (65);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (70);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (75);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (80);
INSERT INTO MSAs_by_mktshare (mktshare) VALUES (85);



UPDATE MSAs_by_mktshare SET 
    percent_of_MSAs = (SELECT sum((pct_amt_fha >= MSAs_by_mktshare.mktshare)::int)*100.0/count(1) FROM FHA_by_MSA), 
    percent_of_borrowers = (SELECT sum(num_all * (pct_amt_fha >= MSAs_by_mktshare.mktshare)::int)*100.0/sum(num_all) FROM FHA_BY_MSA),
    percent_of_amount = (SELECT sum(amt_all * (pct_amt_fha >= MSAs_by_mktshare.mktshare)::int)*100.0/sum(amt_all) FROM FHA_BY_MSA);

\COPY (SELECT * FROM MSAs_by_mktshare) TO  'data/summary_MSAs_by_mktshare.csv' WITH CSV;
