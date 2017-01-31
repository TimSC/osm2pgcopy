from pyo5m import o5m, osmxml
import gzip, json, config, datetime
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2

def GetWaysForNodes(conn, qids, nodeIds, extraNodeIds, knownWayIds):
	sqlFrags = []
	wayTable = "{0}ways".format(config.dbtableprefix)
	wayMemTable = "{0}way_mems".format(config.dbtableprefix)
	for qid in qids:
		sqlFrags.append("{0}.member = %s".format(wayMemTable))

	sql = "SELECT {0}.* FROM {1} INNER JOIN {0} ON {1}.id = {0}.id AND {1}.version = {0}.version WHERE current = true and visible = true AND ({2});".format(wayTable, wayMemTable, " OR ".join(sqlFrags))

	cur = conn.cursor('ways-for-node-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(sql, qids)
	
	wcount = 0
	for row in cur:
		wid = row["id"]
		if wid not in knownWayIds:
			wcount += 1
			knownWayIds.add(wid)
		for mem in row["members"]:
			if mem not in nodeIds:
				extraNodeIds.add(mem)
	
	cur.close()
	return wcount
	
def GetNodesById(conn, qids, outEnc):
	sqlFrags = []
	for qid in qids:
		sqlFrags.append("id = %s")
	query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}nodes".format(config.dbtableprefix) +
		" WHERE ({0}) and visible=true and current=true;".format(" OR ".join(sqlFrags)))
	cur = conn.cursor('node-by-id-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query, qids)
	count = 0

	for row in cur:
		count += 1
		nid = row["id"]
		metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
			row["changeset"], row["uid"], row["username"], row["visible"])
		enc.StoreNode(nid, metaData, row["tags"], (row["lat"], row["lon"]))

	cur.close()

def GetWaysById(conn, qids, outEnc):
	sqlFrags = []
	for qid in qids:
		sqlFrags.append("id = %s")
	query = ("SELECT * FROM {0}ways".format(config.dbtableprefix) +
		" WHERE ({0}) and visible=true and current=true;".format(" OR ".join(sqlFrags)))
	cur = conn.cursor('way-by-id-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query, qids)
	count = 0

	for row in cur:
		count += 1
		wid = row["id"]
		metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
			row["changeset"], row["uid"], row["username"], row["visible"])
		enc.StoreWay(wid, metaData, row["tags"], row["members"])

	cur.close()

def GetRelationsForObjects(conn, qtype, qids, knownRelationIds, relationIdsOut, encOut):
	#print qids
	sqlFrags = []
	relTable = "{0}relations".format(config.dbtableprefix)
	relMemTable = "{0}relation_mems_{1}".format(config.dbtableprefix, qtype)
	for qid in qids:
		sqlFrags.append("{0}.member = %s".format(relMemTable))
	sql = "SELECT {0}.* FROM {1} INNER JOIN {0} ON {1}.id = {0}.id AND {1}.version = {0}.version WHERE current = true and visible = true AND ({2});".format(relTable, relMemTable, " OR ".join(sqlFrags));

	cur = conn.cursor('relation-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(sql, qids)
	count = 0
	for row in cur:
		count += 1
		rid = row["id"]
		if rid not in knownRelationIds:
			relationIdsOut.add(rid)
			mems = []
			for (memTy, memId), memRole in zip(row["members"], row["memberroles"]):
				mems.append((memTy, memId, memRole))
			metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
				row["changeset"], row["uid"], row["username"], row["visible"])
			enc.StoreRelation(rid, metaData, row["tags"], mems)
			knownRelationIds.add(rid)

	cur.close()

def ShpFileToLineString(fina):
	ptStrs = []
	for pt in open(fina, "rt").readlines():
		try:
			latlon = map(float, pt.strip().split(" "))
		except:
			continue #Invalid line
		ptStrs.append("{0} {1}".format(latlon[1], latlon[0]))
	ptStrs.append(ptStrs[0]) #Close loop
	shpStr = "POLYGON(({0}))".format(",".join(ptStrs))
	return shpStr

if __name__=="__main__":
	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))
	#left,bottom,right,top
	bbox = None
	bbox = [20.8434677,39.6559274,20.8699036,39.6752201] #Town in greece
	#bbox = [108.4570313, -45.9511497, 163.4765625, -8.5810212] #Australia
	#bbox = [-16.6113281,49.6676278,2.3730469,62.6741433] #UK and Ireland
	
	shpStr = None
	#shpStr = ShpFileToLineString("shapes/hayling.shp")
	#shpStr = ShpFileToLineString("shapes/ontario.shp")

	if 1:
		fi = gzip.open("extract.o5m.gz", "wb")
		enc = o5m.O5mEncode(fi)
	if 0:
		fi = gzip.open("extract.osm.gz", "wb")
		enc = osmxml.OsmXmlEncode(fi)
	if 0:
		fi = open("extract.osm", "wt")
		enc = osmxml.OsmXmlEncode(fi)
	enc.StoreIsDiff(False)
	if bbox is not None:
		enc.StoreBounds(bbox)

	#Get nodes within bbox
	knownNodeIds = set()

	if bbox is not None:
		query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}nodes".format(config.dbtableprefix) +
			" WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326) " +
			" and visible=true and current=true;")
		cur = conn.cursor('node-cursor', cursor_factory=psycopg2.extras.DictCursor)
		psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
		cur.execute(query, bbox)
	else:
		query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}nodes".format(config.dbtableprefix) +
			" WHERE ST_Contains(ST_GeomFromText(%s, 4326), geom) " +
			" and visible=true and current=true;")
		cur = conn.cursor('node-cursor', cursor_factory=psycopg2.extras.DictCursor)
		psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
		cur.execute(query, [shpStr])

	for row in cur:
		nid = row["id"]
		knownNodeIds.add(nid)
		metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
			row["changeset"], row["uid"], row["username"], row["visible"])
		enc.StoreNode(nid, metaData, row["tags"], (row["lat"], row["lon"]))

	cur.close()
	print "num nodes", len(knownNodeIds)

	#Get ways for these nodes
	cursor = 0
	step = 100

	qids = []
	extraNodeIds = set()
	knownWayIds = set()
	for qid in knownNodeIds:
		qids.append(qid)
		if len(qids) >= step:
			GetWaysForNodes(conn, qids, knownNodeIds, extraNodeIds, knownWayIds)
			qids = []
	if len(qids) > 0:
		GetWaysForNodes(conn, qids, knownNodeIds, extraNodeIds, knownWayIds)

	print "num ways", len(knownWayIds)
	print "extraNodeIds", len(extraNodeIds)

	#Get nodes to complete ways
	qids = []
	cursor = 0
	for qid in extraNodeIds:
		qids.append(qid)
		if len(qids) >= step:
			GetNodesById(conn, qids, enc)
			qids = []
	if len(qids) > 0:
		GetNodesById(conn, qids, enc)

	knownNodeIds.update(extraNodeIds)
	del extraNodeIds
	enc.Reset()
	print "nodes to complete ways done"

	#Send ways to output encoder
	print "encoding ways"
	qids = []
	cursor = 0
	for qid in knownWayIds:
		qids.append(qid)
		if len(qids) >= step:
			GetWaysById(conn, qids, enc)
			qids = []
	if len(qids) > 0:
		GetWaysById(conn, qids, enc)

	enc.Reset()
	print "ways encoded"

	#Get relations for these objects
	cursor = 0
	qids = []
	knownRelationIds = set()
	for qid in knownNodeIds:
		qids.append(qid)
		if len(qids) >= step:
			GetRelationsForObjects(conn, "n", qids, knownRelationIds, knownRelationIds, enc)
			qids = []
	if len(qids) > 0:
		GetRelationsForObjects(conn, "n", qids, knownRelationIds, knownRelationIds, enc)
	print "num relations from nodes", len(knownRelationIds)
	
	qids = []
	cursor = 0
	relationsFromWays = set()
	for qid in knownWayIds:
		qids.append(qid)
		if len(qids) >= step:
			GetRelationsForObjects(conn, "w", qids, knownRelationIds, relationsFromWays, enc)
			qids = []
	if len(qids) > 0:
		GetRelationsForObjects(conn, "w", qids, knownRelationIds, relationsFromWays, enc)
	print "num relations from ways", len(relationsFromWays)
	knownRelationIds.update(relationsFromWays)

	#Get relations of relations
	seekingRelIds = knownRelationIds
	for i in range(10):
		cursor = 0
		qids = []
		extraRelationIds = set()
		for qid in seekingRelIds:
			qids.append(qid)
			if len(qids) >= step:
				GetRelationsForObjects(conn, "r", qids, knownRelationIds, extraRelationIds, enc)
				qids = []
		if len(qids) > 0:
			GetRelationsForObjects(conn, "r", qids, knownRelationIds, extraRelationIds, enc)
		print "extraRelationIds", len(extraRelationIds)
		
		knownRelationIds.update(extraRelationIds)
		if len(extraRelationIds) == 0:
			break
		seekingRelIds = extraRelationIds

	enc.Finish()
	fi.close()
	print "All done"


