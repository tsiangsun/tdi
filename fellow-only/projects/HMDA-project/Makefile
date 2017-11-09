# This is a simple data pipeline:
#
# Ways in which it is simple:
# - it assumes local data storage (i.e., we never have to query a server to see if something's changed);
# - we do only simple dependencies between stages (e.g., we don't worry about fetching different parts of the same dataset in parallel)
#
# Ways in which it is not:
# - we use a database to store out data, so the targets aren't really files in the ordinary sense; as a result,
# - we use 'sentinel files' -- that is files with no content, but whose presence and timestamp indicate that some operation has been completed
#

# We're going to declare the command we use to talk to the database
PSQL_HMDA=psql --set ON_ERROR_STOP=1 hmda-lar
all: data/.make_mktshare_summary

# Make a summary of the FHA marketshare data
data/.make_mktshare_summary: data/.make_hmda_mktshare src/db_hmda/make_summary.sql
	cat src/db_hmda/make_summary.sql | ${PSQL_HMDA}
	touch $@

# Do some processing on the HMDA data, to pull out FHA marketshare by MSA / census tract
data/.make_hmda_mktshare: data/.make_hmda_db src/db_hmda/analyze_geo_msa.sql src/db_hmda/analyze_geo_tract.sql
	cat src/db_hmda/analyze_geo_msa.sql | ${PSQL_HMDA}
	cat src/db_hmda/analyze_geo_tract.sql | ${PSQL_HMDA}
	touch $@

## Downloading HMDA data and loading it into the DB.  The data dictionaries (in dicts/) are hand-made.
data/.make_hmda_db: data/.make_hmda_dl src/db_hmda/make_tables.sql dicts/agencies.csv dicts/loan-types.csv src/db_hmda/python-na.py
	dropdb --if-exists hmda-lar
	createdb hmda-lar
	cat src/db_hmda/make_tables.sql | ${PSQL_HMDA}
	cat "data/raw/hmda/2012PMICLAR_-_National.zip" | funzip | src/db_hmda/python-na.py | ${PSQL_HMDA} -c "COPY PMIC_LAR FROM stdin WITH DELIMITER ',' CSV NULL AS '';"
	cat "data/raw/hmda/2012HMDALAR_-_National.zip" | funzip | src/db_hmda/python-na.py | ${PSQL_HMDA} -c "COPY HMDA_LAR FROM stdin WITH DELIMITER ',' CSV NULL AS '';"
	touch $@

data/.make_hmda_dl: src/get_hmda_2012.sh
	bash src/get_hmda_2012.sh
	curl http://www2.census.gov/geo/docs/maps-data/data/gazetteer/2013_Gazetteer/2013_Gaz_tracts_national.zip | funzip > data/raw/2013_Gaz_tracts_national.tsv
	touch $@

### Cleanup...
clean:
	rm -r data/*
	rm -f data/.make_*

# If we need to
drop_hmda_db:
	dropdb --if-exists hmda-lar
	rm -f data/.make_hmda_db
