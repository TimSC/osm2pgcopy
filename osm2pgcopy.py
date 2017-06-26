from pyo5m import osmxml, o5m
import gzip, json, sys, hashlib, os, bz2

#select * from pg_stat_activity;

#SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM andorra_nodes WHERE geom && ST_MakeEnvelope(1.5020099, 42.5228903, 1.540173, 42.555443, 4326);
#GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO microcosm;

#CREATE INDEX nodes_gin ON nodes USING GIN (tags);
#SELECT * FROM nodes WHERE tags ? 'amenity' LIMIT 10;

#Import from fosm.org 2015 dump
#       table_name        | row_estimate | toast_bytes | table_bytes  |   total    |   index    |   toast    |   table    
#-------------------------+--------------+-------------+--------------+------------+------------+------------+------------
# planet_nodes            |  1.36598e+09 |      131072 | 166346784768 | 252 GB     | 98 GB      | 128 kB     | 155 GB
# planet_ways             |  1.23554e+08 |   548945920 |  48414597120 | 48 GB      | 2647 MB    | 524 MB     | 45 GB
# planet_relations        |  1.39281e+06 |    14221312 |    653623296 | 1082 MB    | 445 MB     | 14 MB      | 623 MB
# planet_way_mems         |   1.6108e+09 |             |  84069392384 | 112 GB     | 34 GB      |            | 78 GB
# planet_relation_mems_w  |   1.2219e+07 |             |    637747200 | 870 MB     | 262 MB     |            | 608 MB
# planet_relation_mems_r  |       112434 |             |      5898240 | 8256 kB    | 2496 kB    |            | 5760 kB
# planet_relation_mems_n  |  1.78653e+06 |             |     93265920 | 127 MB     | 38 MB      |            | 89 MB

#COPY planet_relations TO PROGRAM 'gzip > /home/postgres/dumprelations.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');

class CsvStore(object):

	def __init__(self, outPrefix):
		self.nodeFile = gzip.GzipFile(outPrefix+"nodes.csv.gz", "wb")
		self.wayFile = gzip.GzipFile(outPrefix+"ways.csv.gz", "wb")
		self.wayMembersFile = gzip.GzipFile(outPrefix+"waymems.csv.gz", "wb")
		self.relationFile = gzip.GzipFile(outPrefix+"relations.csv.gz", "wb")
		self.relationMemNodesFile = gzip.GzipFile(outPrefix+"relationmems-n.csv.gz", "wb")
		self.relationMemWaysFile = gzip.GzipFile(outPrefix+"relationmems-w.csv.gz", "wb")
		self.relationMemRelsFile = gzip.GzipFile(outPrefix+"relationmems-r.csv.gz", "wb")

		self.nodeHash = hashlib.md5()
		self.wayHash = hashlib.md5()
		self.relationHash = hashlib.md5()

	def Close(self):
		self.nodeFile.close()
		self.wayFile.close()
		self.wayMembersFile.close()
		self.relationFile.close()
		self.relationMemNodesFile.close()
		self.relationMemWaysFile.close()
		self.relationMemRelsFile.close()

	def FuncStoreNode(self, objectId, metaData, tags, pos):
		version, timestamp, changeset, uid, username, visible = metaData
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
		if visible is None: visible = True
		current = True
		li = u'{0},{3},{4},{5},{6},{7},{8},{9},\"{10}\",SRID=4326;POINT({1} {2})\n'. \
			format(objectId, pos[1], pos[0], changeset, username, uid, visible, \
			timestamp, version, current, tagDump).encode("UTF-8")
		self.nodeHash.update(li)
		self.nodeFile.write(li)

	def FuncStoreWay(self, objectId, metaData, tags, refs):
		version, timestamp, changeset, uid, username, visible = metaData
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

		memDump= json.dumps(refs)
		if visible is None: visible = True
		current = True
		li = u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\"\n'. \
			format(objectId, changeset, username, uid, visible, \
			timestamp, version, current, tagDump, memDump).encode("UTF-8")
		self.wayHash.update(li)
		self.wayFile.write(li)

		for mem in refs:
			self.wayMembersFile.write("{0},{1},{2}\n".format(objectId, version, mem))

	def FuncStoreRelation(self, objectId, metaData, tags, refs):
		version, timestamp, changeset, uid, username, visible = metaData
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

		memDump= json.dumps([mem[:2] for mem in refs])
		memDump = memDump.replace('"', '""')
		rolesDump= json.dumps([mem[2] for mem in refs])
		rolesDump = rolesDump.replace('"', '""')
		if visible is None: visible = True
		current = True
		li = u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\",\"{10}\"\n'. \
			format(objectId, changeset, username, uid, visible, \
			timestamp, version, current, tagDump, memDump, rolesDump).encode("UTF-8")
		self.relationHash.update(li)
		self.relationFile.write(li)

		for memTy, memId, memRole in refs:
			if memTy == "node":
				self.relationMemNodesFile.write("{0},{1},{2}\n".format(objectId, version, memId))
			elif memTy == "way":
				self.relationMemWaysFile.write("{0},{1},{2}\n".format(objectId, version, memId))
			elif memTy == "relation":
				self.relationMemRelsFile.write("{0},{1},{2}\n".format(objectId, version, memId))

if __name__=="__main__":

	inFina = "/home/tim/osm/earth201507161012.osm.gz"
	outPrefix = ""
	if len(sys.argv) >= 2:
		inFina = sys.argv[1]
	if len(sys.argv) >= 3:
		outPrefix = sys.argv[2]

	splitFina = inFina.split(".")
	if splitFina[-1] == "gz":
		fi = gzip.open(inFina, "rt")
	elif splitFina[-1] == "bz2":
		fi = bz2.BZ2File(inFina, "r")
	else:
		fi = open(inFina, "rt")

	if splitFina[-2] == "o5m":
		dec = o5m.O5mDecode(fi)
		dec.DecodeHeader()
	else:
		dec = osmxml.OsmXmlDecode(fi)

	csvStore = CsvStore(outPrefix)
	dec.funcStoreNode = csvStore.FuncStoreNode
	dec.funcStoreWay = csvStore.FuncStoreWay
	dec.funcStoreRelation = csvStore.FuncStoreRelation

	print "Note: Output files not flushed, valid or complete until this program has finished!"

	done = False
	while not done:
		done = dec.DecodeNext()

	csvStore.Close()

	print "nodeHash", csvStore.nodeHash.hexdigest()
	print "wayHash", csvStore.wayHash.hexdigest()
	print "relationHash", csvStore.relationHash.hexdigest()

	del csvStore
	del dec

