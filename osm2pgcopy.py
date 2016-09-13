from pyo5m import osmxml
import gzip, json

#DROP TABLE test;
#CREATE TABLE IF NOT EXISTS test (id BIGINT, changeset BIGINT, username TEXT, uid INTEGER, visible BOOLEAN, timestamp BIGINT, version INTEGER, current BOOLEAN, tags TEXT, geom GEOMETRY(Point, 4326));

class CsvStore(object):

	def __init__(self):
		self.nodeFile = gzip.GzipFile("nodes.csv.gz", "wb")
		
	def __del__(self):
		self.nodeFile.close()

	def FuncStoreNode(self, objectId, metaData, tags, pos):
		version, timestamp, changeset, uid, username = metaData
		tagDump = json.dumps(tags)
		tagDump = tagDump.replace('"', '')
		#tagDump = "stuff"
		if username is not None:
			username = '"'+username.replace('"', '\\"')+'"'
		else:
			username = "NULL"
		visible = True
		current = True
		self.nodeFile.write('{0},{3},{4},{5},{6},{7},{8},{9},\"{10}\",SRID=4326;POINT({1} {2})\n'.
			format(objectId, pos[1], pos[0], changeset, username, uid, visible, 
			timestamp.strftime("%s"), version, current, tagDump))

	def FuncStoreWay(self, objectId, metaData, tags, refs):
		#print objectId, metaData, tags, refs
		pass
	def FuncStoreRelation(self, objectId, metaData, tags, refs):
		#print objectId, metaData, tags, refs
		pass

if __name__=="__main__":

	fi = open("test.osm", "rt")
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

