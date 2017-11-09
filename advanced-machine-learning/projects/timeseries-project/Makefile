TOLYDROP_DATA=http://s3.amazonaws.com/thedataincubator/coursedata/

all: data/raw/daily_country.csv data/raw/temperatures.csv

data/raw/daily_country.csv:
	mkdir -p data/raw/ || true
	if [ ! -e data/raw/daily_country.csv ]; then curl ${TOLYDROP_DATA}gdelt-data-zip/daily_country.csv.gz | zcat > data/raw/daily_country.csv; fi

data/raw/temperatures.csv: src/get-temperatures.py
	mkdir -p data/raw/ || true
	python src/get-temperatures.py > data/raw/temperatures.csv

#data/.make_gdelt_dailies_dl: src/gdelt/get_gdelt_dailies.py
#	# TODO!
#	touch $@

### Cleanup...
clean:
	rm -rf data/*
	rm -f data/.make_*
