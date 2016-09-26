import gzip, json, datetime
from pyo5m import OsmData
import psycopg2 #apt install python-psycopg2
import config

def ProcessFile(fina, conn):

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
			version, timestamp, changeset, uid, username, visible = metaData

			cur = conn.cursor()
			cur.execute("""SELECT version FROM {0}nodes WHERE id = %s;""".format(config.dbtableprefix), (objectId,))
			dbVersions = [tmp[0] for tmp in cur.fetchall()]
			if version in dbVersions:
				nodeHits += 1
			else:
				#print "node missing", objectId, version, dbVersions
				foundNewest = len(dbVersions) == 0 or version > max(dbVersions)
				if foundNewest and len(dbVersions) > 0:
					#Set older versions to not be current
					cur = conn.cursor()
					cur.execute("""UPDATE {0}nodes SET current=false WHERE id = %s;""".format(config.dbtableprefix), (objectId,))

				cur = conn.cursor()
				geom = "POINT({0} {1})".format(pos[1], pos[0])
				unixTime = timestamp.strftime("%s")
				insertSql = "INSERT INTO {0}nodes ".format(config.dbtableprefix)+\
					"(id, changeset, username, uid, visible, timestamp, version, current, tags, geom) "+\
					"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_GeomFromText(%s, 4326));"
				cur.execute(insertSql, (objectId, changeset, username, 
					uid, visible, unixTime, version, foundNewest, json.dumps(tags), geom))
				#print cur.rowcount

		conn.commit()
		print "nodes", nodeHits, len(osmData.nodes)

		wayHits = 0
		for objectId, metaData, tags, refs in osmData.ways:
			#print objectId, metaData, tags, refs
			version, timestamp, changeset, uid, username, visible = metaData

			cur = conn.cursor()
			cur.execute("""SELECT version FROM {0}ways WHERE id = %s;""".format(config.dbtableprefix), (objectId,))
			dbVersions = [tmp[0] for tmp in cur.fetchall()]
			if version in dbVersions:
				wayHits += 1
			else:
				#print "way missing", objectId, version, dbVersions
				foundNewest = len(dbVersions) == 0 or version > max(dbVersions)
				if foundNewest and len(dbVersions) > 0:
					#Set older versions to not be current
					cur = conn.cursor()
					cur.execute("""UPDATE {0}ways SET current=false WHERE id = %s;""".format(config.dbtableprefix), (objectId,))

				cur = conn.cursor()
				unixTime = timestamp.strftime("%s")
				insertSql = "INSERT INTO {0}ways ".format(config.dbtableprefix)+\
					"(id, changeset, username, uid, visible, timestamp, version, current, tags, members) "+\
					"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
				cur.execute(insertSql, (objectId, changeset, username, uid, visible, unixTime, version, foundNewest, 
					json.dumps(tags), 
					json.dumps(refs)))

		conn.commit()
		print "ways", wayHits, len(osmData.ways)

		relationHits = 0
		for objectId, metaData, tags, refs in osmData.relations:
			version, timestamp, changeset, uid, username, visible = metaData

			cur = conn.cursor()
			cur.execute("""SELECT version FROM {0}relations WHERE id = %s;""".format(config.dbtableprefix), (objectId,))
			dbVersions = [tmp[0] for tmp in cur.fetchall()]
			if version in dbVersions:
				relationHits += 1
			else:
				#print "relation missing", objectId, version, dbVersions
				foundNewest = len(dbVersions) == 0 or version > max(dbVersions)
				if foundNewest and len(dbVersions) > 0:
					#Set older versions to not be current
					cur = conn.cursor()
					cur.execute("""UPDATE {0}relations SET current=false WHERE id = %s;""".format(config.dbtableprefix), (objectId,))

				cur = conn.cursor()
				unixTime = timestamp.strftime("%s")
				mems = []
				memroles = []
				for memTy, memId, memRole in refs:
					mems.append([memTy, memId])
					memroles.append(memRole)
				insertSql = "INSERT INTO {0}relations ".format(config.dbtableprefix)+\
					"(id, changeset, username, uid, visible, timestamp, version, current, tags, members, memberroles) "+\
					"VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
				cur.execute(insertSql, (objectId, changeset, username, uid, 
					visible, unixTime, version, foundNewest, 
					json.dumps(tags), 
					json.dumps(mems), 
					json.dumps(memroles)))

		conn.commit()
		print "relations", relationHits, len(osmData.relations)

if __name__ == "__main__":

	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))

	#102/552 : Day before planet dump
	#102/556 : All new data

	for i in range(552, 1000):
		for j in range(0, 1000):
			fina = "102/{0:03d}/{1:03d}.osc.gz".format(i, j)
			ProcessFile(fina, conn)

	#for i in range(0, 184):
	#	for j in range(0, 1000):
	#		fina = "103/{0:03d}/{1:03d}.osc.gz".format(i, j)
	#		ProcessFile(fina, conn)

