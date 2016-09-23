import gzip
from pyo5m import OsmData
import psycopg2 #apt install python-psycopg2
import config

if __name__ == "__main__":

	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))

	#552 : Day before planet dump

	for i in range(0, 1000):
		fina = "102/552/{0:03d}.osc.gz".format(i)
		fi = gzip.open(fina) 
		osmData = OsmData.OsmData()
		osmData.LoadFromOsmXml(fi)

		if len(osmData.nodes) == 0 and len(osmData.ways) == 0 and len(osmData.relations) == 0:
			continue
		print fina

		nodeHits = 0
		for objectId, metaData, tags, pos in osmData.nodes:
			#print objectId, metaData, tags, pos
			version, timestamp, changeset, uid, username = metaData

			cur = conn.cursor()
			cur.execute("""SELECT COUNT(*) FROM planet_nodes WHERE id = %s AND version = %s;""", (objectId, version))
			count = cur.fetchall()[0]
			if count > 0:
				nodeHits += 1
			#print objectId, version, count

		print "nodes", nodeHits, len(osmData.nodes)

		wayHits = 0
		for objectId, metaData, tags, refs in osmData.ways:
			#print objectId, metaData, tags, refs
			version, timestamp, changeset, uid, username = metaData

			cur = conn.cursor()
			cur.execute("""SELECT COUNT(*) FROM planet_ways WHERE id = %s AND version = %s;""", (objectId, version))
			count = cur.fetchall()[0]
			if count > 0:
				wayHits += 1
			#print objectId, version, count

		print "ways", wayHits, len(osmData.ways)

		relationHits = 0
		for objectId, metaData, tags, refs in osmData.relations:
			version, timestamp, changeset, uid, username = metaData

			cur = conn.cursor()
			cur.execute("""SELECT COUNT(*) FROM planet_relations WHERE id = %s AND version = %s;""", (objectId, version))
			count = cur.fetchall()[0]
			if count > 0:
				relationHits += 1
			#print objectId, version, count

		print "relations", relationHits, len(osmData.relations)

