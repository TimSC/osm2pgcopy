from __future__ import print_function
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2
import sys

def DbExec(cur, sql):
	print (sql)
	cur.execute(sql)

def DropTables(cur, config, p):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	DbExec(cur, "DROP MATERIALIZED VIEW IF EXISTS {0}livenodes;".format(p))
	DbExec(cur, "DROP MATERIALIZED VIEW IF EXISTS {0}liveways;".format(p))
	DbExec(cur, "DROP MATERIALIZED VIEW IF EXISTS {0}liverelations;".format(p))

	DbExec(cur, "DROP TABLE IF EXISTS {0}nodes;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}ways;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relations;".format(p))

	DbExec(cur, "DROP TABLE IF EXISTS {0}way_mems;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_n;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_w;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_r;".format(p))

	conn.commit()

def CreateTables(conn, config, p):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}nodes (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, geom GEOMETRY(Point, 4326));".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}ways (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relations (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB, memberroles JSONB);".format(p))

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}way_mems (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_n (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_w (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_r (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))

	DbExec(cur, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {0};".format(config["dbuser"]))
	
	conn.commit()

def CopyToDb(conn, config, p, filesPrefix):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	
	DbExec(cur, "COPY {0}nodes FROM PROGRAM 'zcat {1}nodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}ways FROM PROGRAM 'zcat {1}ways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}relations FROM PROGRAM 'zcat {1}relations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()

	DbExec(cur, "COPY {0}way_mems FROM PROGRAM 'zcat {1}waymems.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}relation_mems_n FROM PROGRAM 'zcat {1}relationmems-n.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}relation_mems_w FROM PROGRAM 'zcat {1}relationmems-w.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}relation_mems_r FROM PROGRAM 'zcat {1}relationmems-r.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()

def CreateIndices(conn, config, p):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	DbExec(cur, "ALTER TABLE {0}nodes ADD PRIMARY KEY (id, version);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}ways ADD PRIMARY KEY (id, version);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}relations ADD PRIMARY KEY (id, version);".format(p))
	conn.commit()

	DbExec(cur, "CREATE MATERIALIZED VIEW {0}livenodes AS SELECT * FROM {0}nodes WHERE current = true AND visible = true;".format(p))
	conn.commit()
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}livenodes_gix ON {0}livenodes USING GIST (geom);".format(p))
	conn.commit()
	conn.set_session(autocommit=True)
	DbExec(cur, "VACUUM ANALYZE {0}livenodes(geom);".format(p))
	conn.set_session(autocommit=False)
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}livenodes_ids ON {0}livenodes(id);".format(p))
	conn.commit()

	DbExec(cur, "CREATE MATERIALIZED VIEW {0}liveways AS SELECT * FROM {0}ways WHERE current = true AND visible = true;".format(p))
	conn.commit()
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}liveways_ids ON {0}liveways(id);".format(p))
	conn.commit()

	DbExec(cur, "CREATE MATERIALIZED VIEW {0}liverelations AS SELECT * FROM {0}relations WHERE current = true AND visible = true;".format(p))
	conn.commit()
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}liverelations_ids ON {0}liverelations(id);".format(p))
	conn.commit()

	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}way_mems_mids ON {0}way_mems (member);".format(p))
	conn.commit()

	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_n_mids ON {0}relation_mems_n (member);".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_w_mids ON {0}relation_mems_w (member);".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_r_mids ON {0}relation_mems_r (member);".format(p, filesPrefix))
	conn.commit()

	#Is this helpful? Possibly for django?
	#DbExec(cur, "ALTER TABLE {0}relation_mems_n ADD PRIMARY KEY (id, version, index);".format(p))
	#conn.commit()
	#DbExec(cur, "ALTER TABLE {0}relation_mems_w ADD PRIMARY KEY (id, version, index);".format(p))
	#conn.commit()
	#DbExec(cur, "ALTER TABLE {0}relation_mems_r ADD PRIMARY KEY (id, version, index);".format(p))
	#conn.commit()

def GetMaxIds(conn, config, p):

	query = "SELECT MAX(id) FROM {0}nodes".format(p)
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query)
	for row in cur:
		print ("max node id:", row[0])

	query = "SELECT MAX(id) FROM {0}ways".format(p)
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query)
	for row in cur:
		print ("max way id:", row[0])

	query = "SELECT MAX(id) FROM {0}relations".format(p)
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query)
	for row in cur:
		print ("max relation id:", row[0])

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
	print (config["dbtablemodifyprefix"])

	running = True
	
	while running:

		print ("Options:")
		print ("1. Drop old tables in db")
		print ("2. Create tables in db")
		print ("3. Copy data from csv files to db")
		print ("4. Create indices")
		print ("5. Get max object ids")
		print ("6. Drop old modify tables in db")
		print ("7. Create modify tables and indicies in db")
		print ("q. Quit")

		userIn = raw_input()

		if userIn == "1":
			DropTables(conn, config, config["dbtableprefix"])
		elif userIn == "2":
			CreateTables(conn, config, config["dbtableprefix"])
		elif userIn == "3":
			CopyToDb(conn, config, config["dbtableprefix"], filesPrefix)
		elif userIn == "4":
			CreateIndices(conn, config, config["dbtableprefix"])
		elif userIn == "5":
			GetMaxIds(conn, config)
		elif userIn == "6":
			DropTables(conn, config, config["dbtablemodifyprefix"])
		elif userIn == "7":
			CreateTables(conn, config, config["dbtablemodifyprefix"])
			CreateIndices(conn, config, config["dbtablemodifyprefix"])
		elif userIn == "q":
			running = False


