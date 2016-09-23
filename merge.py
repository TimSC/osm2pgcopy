import gzip
from pyo5m import OsmData
import psycopg2 #apt install python-psycopg2
import config
	

def available_versions(conn, objType, objId):
	cur = conn.cursor()
	cur.execute("""SELECT version FROM planet_nodes WHERE id = %s;""", (objId,))
	print list(cur.fetchall())

if __name__ == "__main__":

	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))

	#102/552 : Day before planet dump
	#102/556 : All new data

	for i in range(0, 1000):
		fina = "102/552/{0:03d}.osc.gz".format(i)
		fi = gzip.open(fina) 
		oscData = OsmData.OsmChange()
		oscData.LoadFromOscXml(fi)

		for actionType in ["create", "modify", "delete"]:
			if actionType == "create":
				osmData = oscData.create
			if actionType == "modify":
				osmData = oscData.modify
			if actionType == "delete":
				osmData = oscData.delete

			if len(osmData.nodes) == 0 and len(osmData.ways) == 0 and len(osmData.relations) == 0:
				continue
			print fina, actionType

			nodeHits = 0
			for objectId, metaData, tags, pos in osmData.nodes:
				#print objectId, metaData, tags, pos
				version, timestamp, changeset, uid, username = metaData

				cur = conn.cursor()
				cur.execute("""SELECT COUNT(*) FROM planet_nodes WHERE id = %s AND version >= %s;""", (objectId, version))
				count = cur.fetchall()[0][0]
				if count > 0:
					nodeHits += 1
				#else:
					#print "node missing", objectId, version
					#available_versions(conn, "node", objectId)
				#print objectId, version, count

			print "nodes", nodeHits, len(osmData.nodes)

			wayHits = 0
			for objectId, metaData, tags, refs in osmData.ways:
				#print objectId, metaData, tags, refs
				version, timestamp, changeset, uid, username = metaData

				cur = conn.cursor()
				cur.execute("""SELECT COUNT(*) FROM planet_ways WHERE id = %s AND version >= %s;""", (objectId, version))
				count = cur.fetchall()[0][0]
				if count > 0:
					wayHits += 1
				#print objectId, version, count

			print "ways", wayHits, len(osmData.ways)

			relationHits = 0
			for objectId, metaData, tags, refs in osmData.relations:
				version, timestamp, changeset, uid, username = metaData

				cur = conn.cursor()
				cur.execute("""SELECT COUNT(*) FROM planet_relations WHERE id = %s AND version >= %s;""", (objectId, version))
				count = cur.fetchall()[0][0]
				if count > 0:
					relationHits += 1
				#print objectId, version, count

			print "relations", relationHits, len(osmData.relations)

