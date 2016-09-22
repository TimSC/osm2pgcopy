from pyo5m import osmxml
import gzip, json, sys, hashlib, os, bz2

#sudo su postgres
#psql --dbname=db_map
#
#DROP TABLE IF EXISTS nodes;
#DROP TABLE IF EXISTS ways;
#DROP TABLE IF EXISTS relations;
#CREATE TABLE IF NOT EXISTS nodes (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, geom GEOMETRY(Point, 4326));
#CREATE TABLE IF NOT EXISTS ways (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB);
#CREATE TABLE IF NOT EXISTS relations (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags JSONB, members JSONB);

#COPY nodes FROM '/home/postgres/copytest.csv' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');
#COPY nodes FROM PROGRAM 'zcat /home/postgres/nodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');

#db_map=# COPY nodes FROM PROGRAM 'zcat /home/postgres/nodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');
#ERROR:  missing data for column "changeset"
#CONTEXT:  COPY nodes, line 1365975728: "27549"
#db_map=#

#CREATE INDEX nodes_id ON nodes (id);
#CREATE INDEX ways_id ON ways (id);
#CREATE INDEX relations_id ON relations (id);
#CREATE INDEX nodes_gix ON nodes USING GIST (geom);

#SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM andorra_nodes WHERE geom && ST_MakeEnvelope(1.5020099, 42.5228903, 1.540173, 42.555443, 4326);
#GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO microcosm;

#CREATE INDEX nodes_gin ON nodes USING GIN (tags);
#SELECT * FROM nodes WHERE tags ? 'amenity' LIMIT 10;

#CREATE INDEX way_members ON ways USING GIN (members);
#SELECT members FROM ways WHERE members @> '579973777' LIMIT 10;
#SELECT members FROM andorra_ways WHERE members @> ANY(ARRAY['579973777','51116315']::jsonb[]);

#CREATE INDEX relation_members ON relations USING GIN (members);
#SELECT id FROM greece_relations WHERE members @> '[["way", 10741857, "outer"]]' LIMIT 1;
#SELECT id, arr FROM greece_relations r, jsonb_array_elements(r.members) AS arr WHERE arr->0 ? 'way' AND arr->1 = '24030116'; #Slow!

class CsvStore(object):

	def __init__(self, outPrefix):
		self.nodeFile = gzip.GzipFile(outPrefix+"nodes.csv.gz", "wb")
		self.wayFile = gzip.GzipFile(outPrefix+"ways.csv.gz", "wb")
		self.relationFile = gzip.GzipFile(outPrefix+"relations.csv.gz", "wb")
		self.deltaEncode = False
		self.nodeHash = hashlib.md5()
		self.wayHash = hashlib.md5()
		self.relationHash = hashlib.md5()

	def __del__(self):
		self.Close()

	def Close(self):
		if self.nodeFile is not None:
			self.nodeFile.close()
		if self.wayFile is not None:
			self.wayFile.close()
		if self.relationFile is not None:
			self.relationFile.close()
		self.nodeFile = None
		self.wayFile = None
		self.relationFile = None

	def FuncStoreNode(self, objectId, metaData, tags, pos):
		version, timestamp, changeset, uid, username = metaData
		tagDump = json.dumps(tags)
		tagDump = tagDump.replace('"', '""')
		tagDump = tagDump.replace('\u00b0', '')
		if username is not None:
			username = '"'+username.replace('"', '""')+'"'
		else:
			username = "NULL"
		if uid is None:
			uid = "NULL"
		if changeset is None:
			changeset = "NULL"
		if timestamp is not None:
			timestamp = timestamp.strftime("%s")
		else:
			timestamp = "NULL"
		visible = True
		current = True
		li = u'{0},{3},{4},{5},{6},{7},{8},{9},\"{10}\",SRID=4326;POINT({1} {2})\n'. \
			format(objectId, pos[1], pos[0], changeset, username, uid, visible, \
			timestamp, version, current, tagDump).encode("UTF-8")
		self.nodeHash.update(li)
		self.nodeFile.write(li)

	def FuncStoreWay(self, objectId, metaData, tags, refs):
		version, timestamp, changeset, uid, username = metaData
		tagDump = json.dumps(tags)
		tagDump = tagDump.replace('"', '""')
		if username is not None:
			username = '"'+username.replace('"', '""')+'"'
		else:
			username = "NULL"
		if uid is None:
			uid = "NULL"
		if changeset is None:
			changeset = "NULL"
		if timestamp is not None:
			timestamp = timestamp.strftime("%s")
		else:
			timestamp = "NULL"
		if self.deltaEncode:
			if len(refs) > 0:
				deltaRefs = [refs[0]]
				currentRef = refs[0]
				for r in refs[1:]:
					deltaRefs.append(r - currentRef)
					currentRef = r
			else:
				deltaRefs = []
			memDump= json.dumps(deltaRefs)
		else:
			memDump= json.dumps(refs)
		visible = True
		current = True
		li = u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\"\n'. \
			format(objectId, changeset, username, uid, visible, \
			timestamp, version, current, tagDump, memDump).encode("UTF-8")
		self.wayHash.update(li)
		self.wayFile.write(li)

	def FuncStoreRelation(self, objectId, metaData, tags, refs):
		version, timestamp, changeset, uid, username = metaData
		tagDump = json.dumps(tags)
		tagDump = tagDump.replace('"', '""')
		if username is not None:
			username = u'"'+username.replace(u'"', u'""')+u'"'
		else:
			username = u"NULL"
		if uid is None:
			uid = "NULL"
		if changeset is None:
			changeset = "NULL"
		if timestamp is not None:
			timestamp = timestamp.strftime("%s")
		else:
			timestamp = "NULL"
		if self.deltaEncode:
			if len(refs) > 0:
				deltaRefs = [refs[0][0][0], refs[0][1], refs[0][2]]
				currentRef = refs[0][1]
				for r in refs[1:]:
					deltaRefs.append((r[0][0], r[1] - currentRef, r[2]))
					currentRef = r[1]
			else:
				deltaRefs = []
			memDump= json.dumps(deltaRefs)
		else:
			memDump= json.dumps(refs)
		memDump = memDump.replace('"', '""')
		visible = True
		current = True
		li = u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\"\n'. \
			format(objectId, changeset, username, uid, visible, \
			timestamp, version, current, tagDump, memDump).encode("UTF-8")
		self.relationHash.update(li)
		self.relationFile.write(li)

if __name__=="__main__":

	inFina = "/home/tim/osm/earth201507161012.osm.gz"
	outPrefix = ""
	if len(sys.argv) >= 2:
		inFina = sys.argv[1]
	if len(sys.argv) >= 3:
		outPrefix = sys.argv[2]

	splitFina = os.path.splitext(inFina)
	if splitFina[1] == ".gz":
		fi = gzip.open(inFina, "rt")
	elif splitFina[1] == ".bz2":
		fi = bz2.BZ2File(inFina, "r")
	else:
		fi = open(inFina, "rt")
	dec = osmxml.OsmXmlDecode(fi)
	csvStore = CsvStore(outPrefix)
	dec.funcStoreNode = csvStore.FuncStoreNode
	dec.funcStoreWay = csvStore.FuncStoreWay
	dec.funcStoreRelation = csvStore.FuncStoreRelation

	done = False
	while not done:
		done = dec.DecodeNext()

	csvStore.Close()

	print "nodeHash", csvStore.nodeHash.hexdigest()
	print "wayHash", csvStore.wayHash.hexdigest()
	print "relationHash", csvStore.relationHash.hexdigest()

	del csvStore
	del dec

