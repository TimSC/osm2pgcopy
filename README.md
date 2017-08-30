# osm2pgcopy
Convert OSM format to postgresql/postgis COPY csv dump, used for fast import. The schema is used by Microcosm OSM server https://github.com/TimSC/microcosm but it may be useful for other purposes.

Install and configure postgis
=============================

    sudo apt install postgis postgresql postgresql-9.5-postgis-2.2

Create the database and user. Generate your own secret password (the colon character should not be used). The user postgres exists on many linux systems and is the default admin account for postgres.
    
	sudo su postgres

	psql

	CREATE DATABASE db_map;

	CREATE USER microcosm WITH PASSWORD 'myPassword';

	GRANT ALL PRIVILEGES ON DATABASE db_map to microcosm;

Disconnect using ctrl-D. As the user postgres, enable postgis on the database.

    psql --dbname=db_map

	GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO microcosm;

	CREATE EXTENSION postgis;

	CREATE EXTENSION postgis_topology;

	CREATE EXTENSION postgis_sfcgal;

	CREATE EXTENSION address_standardizer;

	CREATE EXTENSION address_standardizer_data_us;
	
Disconnect using ctrl-D to get back to your normal user. Check you can connect using psql; it often works out of the box. 

    psql -h 127.0.0.1 -d db_map -U microcosm --password

If necessary, enable log in by password by changing pg_hba.conf as administrator https://stackoverflow.com/a/4328789/4288232 . When connecting, use 127.0.0.1 rather than localhost, if the database is on the same machine (postgresql treats them differently).

	locate pg_hba.conf

	sudo nano /etc/postgresql/9.5/main/pg_hba.conf

Update the configuration files with your new password. Use 127.0.0.1 rather than localhost, if the database is on the same machine.

	cp config.cfg.template config.cfg

	nano config.cfg

osm to postgresql script
========================

Usually the fastest way to import a database into postgresql/postgis is to convert the data to CSV format, then use a COPY command as an database administrator to transfer it into postgresql. (A binary COPY would be even faster but that has not been implemented.) The following is an example to import greece-latest.osm.bz2 into the database. As your normal user:

    sudo apt install python-dev python-psycopg2

	cd <src>
	
	git submodule init

	git submodule update

	cd pyo5m/

	python setup.py build_ext --inplace

	cd ..
	
	python osm2pgcopy.py greece-latest.osm.bz2 greece-

And wait for a while. You can see the results and change permissions so it can be accessed by the postgres user:

    ls -lh greece-*.csv.gz

	sudo chmod a+rx greece-*.csv.gz

Run the copytodb.py script using the postgres user on the machine running the database (since we are going to be modifying permissions and doing COPY commands). 

	sudo su postgres
	
	python copytodb.py /full/path/to/greece-

copytodb has an interactive command line menu. Select the options presented in order. This creates the tables, copies the data and creates the indices. This can take a while for large areas, and over a day for a whole planet dump!

database dump
=============

The current fastest method is to use the C++ dump program.

	sudo apt install libpqxx-dev rapidjson-dev

	make

	./dump


