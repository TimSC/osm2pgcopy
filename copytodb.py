from __future__ import print_function
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2
import sys

def DbExec(cur, sql):
	print (sql)
	cur.execute(sql)

def DropTables(cur, config):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	DbExec(cur, "DROP TABLE IF EXISTS {0}nodes;".format(config["dbtableprefix"]))
	DbExec(cur, "DROP TABLE IF EXISTS {0}ways;".format(config["dbtableprefix"]))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relations;".format(config["dbtableprefix"]))

	DbExec(cur, "DROP TABLE IF EXISTS {0}way_mems;".format(config["dbtableprefix"]))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_n;".format(config["dbtableprefix"]))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_w;".format(config["dbtableprefix"]))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_r;".format(config["dbtableprefix"]))

def CreateTables(conn, config,):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}nodes (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, geom GEOMETRY(Point, 4326));".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}ways (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB);".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relations (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB, memberroles JSONB);".format(config["dbtableprefix"]))
	DbExec(cur, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {0};".format(config["dbuser"]))

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}way_mems (id BIGINT, version INTEGER, member BIGINT);".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_n (id BIGINT, version INTEGER, member BIGINT);".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_w (id BIGINT, version INTEGER, member BIGINT);".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_r (id BIGINT, version INTEGER, member BIGINT);".format(config["dbtableprefix"]))

def CopyToDb(conn, config, filesPrefix):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	
	DbExec(cur, "COPY {0}nodes FROM PROGRAM 'zcat {1}nodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "COPY {0}ways FROM PROGRAM 'zcat {1}ways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "COPY {0}relations FROM PROGRAM 'zcat {1}relations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(config["dbtableprefix"], filesPrefix))

	DbExec(cur, "COPY {0}way_mems FROM PROGRAM 'zcat {1}waymems.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "COPY {0}relation_mems_n FROM PROGRAM 'zcat {1}relationmems-n.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "COPY {0}relation_mems_w FROM PROGRAM 'zcat {1}relationmems-w.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "COPY {0}relation_mems_r FROM PROGRAM 'zcat {1}relationmems-r.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(config["dbtableprefix"], filesPrefix))

def CreateIndices(conn, config):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}nodes_id ON {0}nodes (id, version);".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}ways_id ON {0}ways (id, version);".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relations_id ON {0}relations (id, version);".format(config["dbtableprefix"]))
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}nodes_gix ON {0}nodes USING GIST (geom);".format(config["dbtableprefix"]))

	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}way_mems_mids ON {0}way_mems (member);".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_n_mids ON {0}relation_mems_n (member);".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_w_mids ON {0}relation_mems_w (member);".format(config["dbtableprefix"], filesPrefix))
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_r_mids ON {0}relation_mems_r (member);".format(config["dbtableprefix"], filesPrefix))

if __name__=="__main__":
	
	configFi = open("config.cfg", "rt")
	config = {}
	for li in configFi.readlines():
		liSplit = li.strip().split(":", 1)
		if len(liSplit) < 2: continue
		config[liSplit[0]] = liSplit[1]

	if len(sys.argv) < 2:
		print ("Specify file prefix as first argument, such as planet-")
		exit(0)
	filesPrefix = sys.argv[1]
	
	conn = psycopg2.connect("dbname='{0}'".format(config["dbname"]))

	print ("Connected to", config["dbname"]+", table prefix", config["dbtableprefix"])

	running = True
	
	while running:

		print ("Options:")
		print ("1. Drop old tables in db")
		print ("2. Create tables in db")
		print ("3. Copy data from csv files to db")
		print ("4. Create indices")
		print ("q. Quit")

		userIn = raw_input()

		if userIn == "1":
			DropTables(conn, config)
		elif userIn == "2":
			CreateTables(conn, config)
		elif userIn == "3":
			CopyToDb(conn, config, filesPrefix)
		elif userIn == "4":
			CreateIndices(conn, config)
		elif userIn == "q":
			running = False


