from __future__ import print_function
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2
import sys

def DbExec(cur, sql):
	print (sql)
	cur.execute(sql)

def DropTables(cur, config, p):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	#Current

	DbExec(cur, "DROP TABLE IF EXISTS {0}oldnodes;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}oldways;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}oldrelations;".format(p))

	DbExec(cur, "DROP TABLE IF EXISTS {0}livenodes;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}liveways;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}liverelations;".format(p))

	DbExec(cur, "DROP TABLE IF EXISTS {0}nodeids;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}wayids;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relationids;".format(p))

	DbExec(cur, "DROP TABLE IF EXISTS {0}way_mems;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_n;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_w;".format(p))
	DbExec(cur, "DROP TABLE IF EXISTS {0}relation_mems_r;".format(p))

	conn.commit()

def CreateTables(conn, config, p):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}oldnodes (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, geom GEOMETRY(Point, 4326));".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}oldways (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}oldrelations (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB, memberroles JSONB);".format(p))

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}livenodes (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, geom GEOMETRY(Point, 4326));".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}liveways (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}liverelations (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, timestamp BIGINT, version INTEGER, tags JSONB, members JSONB, memberroles JSONB);".format(p))

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}nodeids (id BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}wayids (id BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relationids (id BIGINT);".format(p))

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}way_mems (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_n (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_w (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))
	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}relation_mems_r (id BIGINT, version INTEGER, index INTEGER, member BIGINT);".format(p))

	DbExec(cur, "CREATE TABLE IF NOT EXISTS {0}nextids (id VARCHAR(16), maxid BIGINT, PRIMARY KEY(id));".format(p))

	DbExec(cur, "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {0};".format(config["dbuser"]))
	
	conn.commit()

def CopyToDb(conn, config, p, filesPrefix):
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	
	DbExec(cur, "COPY {0}oldnodes FROM PROGRAM 'zcat {1}oldnodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}oldways FROM PROGRAM 'zcat {1}oldways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}oldrelations FROM PROGRAM 'zcat {1}oldrelations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()

	DbExec(cur, "COPY {0}livenodes FROM PROGRAM 'zcat {1}livenodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}liveways FROM PROGRAM 'zcat {1}liveways.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}liverelations FROM PROGRAM 'zcat {1}liverelations.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()

	DbExec(cur, "COPY {0}nodeids FROM PROGRAM 'zcat {1}nodeids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}wayids FROM PROGRAM 'zcat {1}wayids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "COPY {0}relationids FROM PROGRAM 'zcat {1}relationids.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');".format(p, filesPrefix))
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

	DbExec(cur, "ALTER TABLE {0}oldnodes ADD PRIMARY KEY (id, version);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}oldways ADD PRIMARY KEY (id, version);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}oldrelations ADD PRIMARY KEY (id, version);".format(p))
	conn.commit()

	DbExec(cur, "ALTER TABLE {0}livenodes ADD PRIMARY KEY (id);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}liveways ADD PRIMARY KEY (id);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}liverelations ADD PRIMARY KEY (id);".format(p))
	conn.commit()

	DbExec(cur, "ALTER TABLE {0}nodeids ADD PRIMARY KEY (id);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}wayids ADD PRIMARY KEY (id);".format(p))
	conn.commit()
	DbExec(cur, "ALTER TABLE {0}relationids ADD PRIMARY KEY (id);".format(p))
	conn.commit()

	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}livenodes_gix ON {0}livenodes USING GIST (geom);".format(p))
	conn.commit()
	conn.set_session(autocommit=True)
	DbExec(cur, "VACUUM ANALYZE {0}livenodes(geom);".format(p))
	conn.set_session(autocommit=False)

	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}way_mems_mids ON {0}way_mems (member);".format(p))
	conn.commit()

	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_n_mids ON {0}relation_mems_n (member);".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_w_mids ON {0}relation_mems_w (member);".format(p, filesPrefix))
	conn.commit()
	DbExec(cur, "CREATE INDEX IF NOT EXISTS {0}relation_mems_r_mids ON {0}relation_mems_r (member);".format(p, filesPrefix))
	conn.commit()

def GetMaxIds(conn, config, p, p2, p3):

	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	sql = "DELETE FROM {0}nextids;".format(p)
	cur.execute(sql)
	sql = "DELETE FROM {0}nextids;".format(p2)
	cur.execute(sql)
	sql = "DELETE FROM {0}nextids;".format(p3)
	cur.execute(sql)

	maxid = None
	query = "SELECT MAX(id) FROM {0}livenodes".format(p)
	cur.execute(query)
	for row in cur:
		print ("max node id:", row[0])
		maxid = row[0]
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('node', {1});".format(p, maxid+1)
	cur.execute(sql)
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('node', {1});".format(p2, maxid+1)
	cur.execute(sql)
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('node', {1});".format(p3, maxid+1)
	cur.execute(sql)

	query = "SELECT MAX(id) FROM {0}liveways".format(p)
	cur.execute(query)
	for row in cur:
		print ("max way id:", row[0])
		maxid = row[0]
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('way', {1});".format(p, maxid+1)
	cur.execute(sql)
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('way', {1});".format(p2, maxid+1)
	cur.execute(sql)
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('way', {1});".format(p3, maxid+1)
	cur.execute(sql)

	query = "SELECT MAX(id) FROM {0}liverelations".format(p)
	cur.execute(query)
	for row in cur:
		print ("max relation id:", row[0])
		maxid = row[0]
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('relation', {1});".format(p, maxid+1)
	cur.execute(sql)
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('relation', {1});".format(p2, maxid+1)
	cur.execute(sql)
	sql = "INSERT INTO {0}nextids(id, maxid) VALUES ('relation', {1});".format(p3, maxid+1)
	cur.execute(sql)
	conn.commit()

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
		print ("5. Drop old modify tables in db")
		print ("6. Create modify/test tables and indicies in db")
		print ("7. Get max object ids")
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
			DropTables(conn, config, config["dbtablemodifyprefix"])
			DropTables(conn, config, config["dbtabletestprefix"])
		elif userIn == "6":
			CreateTables(conn, config, config["dbtablemodifyprefix"])
			CreateIndices(conn, config, config["dbtablemodifyprefix"])
			CreateTables(conn, config, config["dbtabletestprefix"])
			CreateIndices(conn, config, config["dbtabletestprefix"])
		elif userIn == "7":
			GetMaxIds(conn, config, config["dbtableprefix"], config["dbtabletestprefix"], config["dbtablemodifyprefix"])
		elif userIn == "q":
			running = False

