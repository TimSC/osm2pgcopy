from pyo5m import osmxml
import gzip, json

#DROP TABLE IF EXISTS nodes;
#DROP TABLE IF EXISTS ways;
#DROP TABLE IF EXISTS relations;
#CREATE TABLE IF NOT EXISTS nodes (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags TEXT, geom GEOMETRY(Point, 4326));
#CREATE TABLE IF NOT EXISTS ways (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags TEXT, members TEXT);
#CREATE TABLE IF NOT EXISTS relations (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags TEXT, members TEXT);

#COPY nodes FROM '/home/postgres/copytest.csv' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');
#COPY nodes FROM PROGRAM 'zcat /home/postgres/nodes.csv.gz' WITH (FORMAT 'csv', DELIMITER ',', NULL 'NULL');

class CsvStore(object):

	def __init__(self):
		self.nodeFile = gzip.GzipFile("nodes.csv.gz", "wb")
		self.wayFile = gzip.GzipFile("ways.csv.gz", "wb")
		self.relationFile = gzip.GzipFile("relations.csv.gz", "wb")
		
	def __del__(self):
		self.nodeFile.close()
		self.wayFile.close()
		self.relationFile.close()

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
		visible = True
		current = True
		self.nodeFile.write(u'{0},{3},{4},{5},{6},{7},{8},{9},\"{10}\",SRID=4326;POINT({1} {2})\n'.
			format(objectId, pos[1], pos[0], changeset, username, uid, visible, 
			timestamp.strftime("%s"), version, current, tagDump).encode("UTF-8"))

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
		if len(refs) > 0:
			deltaRefs = [refs[0]]
			currentRef = refs[0]
			for r in refs[1:]:
				deltaRefs.append(r - currentRef)
				currentRef = r
		else:
			deltaRefs = []
		memDump= json.dumps(deltaRefs)
		visible = True
		current = True
		self.wayFile.write(u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\"\n'.
			format(objectId, changeset, username, uid, visible, 
			timestamp.strftime("%s"), version, current, tagDump, memDump).encode("UTF-8"))

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
		if len(refs) > 0:
			deltaRefs = [refs[0][0][0], refs[0][1], refs[0][2]]
			currentRef = refs[0][1]
			for r in refs[1:]:
				deltaRefs.append((r[0][0], r[1] - currentRef, r[2]))
				currentRef = r[1]
		else:
			deltaRefs = []
		memDump= json.dumps(deltaRefs)
		memDump = memDump.replace('"', '""')
		visible = True
		current = True
		self.relationFile.write(u'{0},{1},{2},{3},{4},{5},{6},{7},\"{8}\",\"{9}\"\n'.
			format(objectId, changeset, username, uid, visible, 
			timestamp.strftime("%s"), version, current, tagDump, memDump).encode("UTF-8"))

if __name__=="__main__":

	fi = open("map.osm", "rt")
	dec = osmxml.OsmXmlDecode(fi)
	csvStore = CsvStore()
	dec.funcStoreNode = csvStore.FuncStoreNode
	dec.funcStoreWay = csvStore.FuncStoreWay
	dec.funcStoreRelation = csvStore.FuncStoreRelation

	done = False
	while not done:
		done = dec.DecodeNext()
	
	del csvStore
	del dec

