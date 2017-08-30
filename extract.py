from pyo5m import o5m, osmxml
import gzip, json, config, datetime, sys, time
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2

def GetWaysForNodes(conn, qids, knownNodeIds, extraNodeIds, wayIdsToFind):
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
		if wid not in wayIdsToFind:
			wcount += 1
			wayIdsToFind.add(wid)
		for mem in row["members"]:
			if mem not in knownNodeIds:
				extraNodeIds.add(mem)
	
	cur.close()
	return wcount
	
def GetChildNodesForWays(conn, wayIdsToFind, knownNodeIds, extraNodeIds):
	sqlFrags = []
	wayTable = "{0}ways".format(config.dbtableprefix)
	for qid in wayIdsToFind:
		sqlFrags.append("id = %s")

	sql = "SELECT * FROM {0} WHERE current = true and visible = true AND ({1});".format(wayTable, " OR ".join(sqlFrags))

	cur = conn.cursor('query-ways-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(sql, wayIdsToFind)
	
	for row in cur:
		for mem in row["members"]:
			if mem not in knownNodeIds:
				extraNodeIds.add(mem)
	
	cur.close()

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
		outEnc.StoreNode(nid, metaData, row["tags"], (row["lat"], row["lon"]))

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
		outEnc.StoreWay(wid, metaData, row["tags"], row["members"])

	cur.close()

def GetRelationsById(conn, qids, knownRelationIds, outEnc):
	sqlFrags = []
	for qid in qids:
		if qid in knownRelationIds:
			continue
		sqlFrags.append("id = %s")
	query = ("SELECT * FROM {0}relations".format(config.dbtableprefix) +
		" WHERE ({0}) and visible=true and current=true;".format(" OR ".join(sqlFrags)))
	cur = conn.cursor('relation-by-id-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query, qids)
	count = 0

	for row in cur:
		count += 1
		rid = row["id"]
		mems = []
		for (memTy, memId), memRole in zip(row["members"], row["memberroles"]):
			mems.append((memTy, memId, memRole))
		metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
			row["changeset"], row["uid"], row["username"], row["visible"])
		outEnc.StoreRelation(rid, metaData, row["tags"], mems)

	cur.close()

def GetRelationsMembers(conn, qids, extraNodesOut, extraWaysOut, extraRelationsOut):
	sqlFrags = []
	for qid in qids:
		sqlFrags.append("id = %s")
	query = ("SELECT * FROM {0}relations".format(config.dbtableprefix) +
		" WHERE ({0}) and visible=true and current=true;".format(" OR ".join(sqlFrags)))
	cur = conn.cursor('relation-by-id-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query, qids)
	count = 0

	for row in cur:
		count += 1
		rid = row["id"]
		for (memTy, memId), memRole in zip(row["members"], row["memberroles"]):
			if memTy == "node":
				extraNodesOut.add(memId)
			elif memTy == "way":
				extraWaysOut.add(memId)
			elif memTy == "relation":
				extraRelationsOut.add(memId)
			else:
				raise RuntimeError("Unknown member type: "+str(memTy))
	
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
			encOut.StoreRelation(rid, metaData, row["tags"], mems)
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

def CompleteQuery(conn, queryNodes, knownNodeIds, queryWays, queryRelations, enc):
	queryNodes = set(queryNodes)
	queryWays = set(queryWays)
	queryRelations = set(queryRelations)

	#Get member nodes and ways for queryRelations
	step = 100
	qids = []
	extraNodes, extraWays, extraRelations = set(), set(), set()
	for qid in queryRelations:
		qids.append(qid)
		if len(qids) >= step:
			GetRelationsMembers(conn, qids, extraNodes, extraWays, extraRelations)
			qids = []
	if len(qids) > 0:
		GetRelationsMembers(conn, qids, extraNodes, extraWays, extraRelations)

	#Add query nodes that are not known
	nodesToFind = queryNodes.copy()
	nodesToFind.update(extraNodes)
	nodesToFind = nodesToFind.difference(knownNodeIds)
	
	qids = []
	for qid in nodesToFind:
		qids.append(qid)
		if len(qids) >= step:
			GetNodesById(conn, qids, enc)
			qids = []
	if len(qids) > 0:
		GetNodesById(conn, qids, enc)
	knownNodeIds.update(nodesToFind)
	del nodesToFind

	#Get ways for these nodes, then find node ids to complete them

	qids = []
	extraNodeIds = set()
	wayIdsToFind = queryWays.copy()
	for qid in knownNodeIds:
		qids.append(qid)
		if len(qids) >= step:
			GetWaysForNodes(conn, qids, knownNodeIds, extraNodeIds, wayIdsToFind)
			qids = []
	if len(qids) > 0:
		GetWaysForNodes(conn, qids, knownNodeIds, extraNodeIds, wayIdsToFind)

	print "num ways", len(wayIdsToFind)
	print "extraNodeIds", len(extraNodeIds)

	#Get node ids for query ways
	qids = []
	waysToFind = queryWays.copy()
	waysToFind.update(extraWays)
	for qid in waysToFind:
		qids.append(qid)
		if len(qids) >= step:
			GetChildNodesForWays(conn, qids, knownNodeIds, extraNodeIds)
			qids = []
	if len(qids) > 0:
		GetChildNodesForWays(conn, qids, knownNodeIds, extraNodeIds)

	print "extraNodeIds2", len(extraNodeIds)

	#Get node data to complete ways
	qids = []
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
	for qid in wayIdsToFind:
		qids.append(qid)
		if len(qids) >= step:
			GetWaysById(conn, qids, enc)
			qids = []
	if len(qids) > 0:
		GetWaysById(conn, qids, enc)

	enc.Reset()
	print "ways encoded"

	#Get relations for these objects
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
	relationsFromWays = set()
	for qid in wayIdsToFind:
		qids.append(qid)
		if len(qids) >= step:
			GetRelationsForObjects(conn, "w", qids, knownRelationIds, relationsFromWays, enc)
			qids = []
	if len(qids) > 0:
		GetRelationsForObjects(conn, "w", qids, knownRelationIds, relationsFromWays, enc)
	print "num relations from ways", len(relationsFromWays)

	#Get query relation ids
	qids = []
	relationsFromQueryRelations = set()
	relationsToFind = waysToFind.copy()
	relationsToFind.update(extraRelations)
	for qid in relationsToFind:
		qids.append(qid)
		if len(qids) >= step:
			GetRelationsById(conn, qids, knownRelationIds, enc)
			qids = []
	if len(qids) > 0:
		GetRelationsById(conn, qids, knownRelationIds, enc)	

	#Get relations of relations
	seekingRelIds = knownRelationIds.copy()
	for i in range(10):
		qids = []
		extraRelationIds = set()
		for qid in seekingRelIds:
			qids.append(qid)
			if len(qids) >= step:
				GetRelationsForObjects(conn, "r", qids, knownRelationIds, extraRelationIds, enc)
				qids = []
		if len(qids) > 0:
			GetRelationsForObjects(conn, "r", qids, knownRelationIds, extraRelationIds, enc)
		print i, "extraRelationIds", len(extraRelationIds)
		
		if len(extraRelationIds) == 0:
			break
		seekingRelIds = extraRelationIds


if __name__=="__main__":
	outFina = "extract.osm.gz"

	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))
	#left,bottom,right,top
	bbox = None
	#bbox = [20.8434677,39.6559274,20.8699036,39.6752201] #Town in greece
	#bbox = [108.4570313, -45.9511497, 163.4765625, -8.5810212] #Australia
	#bbox = [-16.6113281,49.6676278,2.3730469,62.6741433] #UK and Ireland
	#bbox = [0.453186,50.8302282,1.4804077,51.5155798] #East Kent, UK
	bbox = [-1.1473846,50.7360206,-0.9901428,50.8649113] #Portsmouth, UK
	#bbox = [-74.734, 44.968, -72.723, 46.057] #montreal_canada

	shpStr = None
	#shpStr = ShpFileToLineString("shapes/hayling.shp")
	#shpStr = ShpFileToLineString("shapes/ontario.shp")

	if 1:
		fi = gzip.open(outFina, "wb")
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
	startTime = time.time()

	if bbox is not None:
		query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}livenodes".format(config.dbtableprefix) +
			" WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326);")
		cur = conn.cursor('node-cursor', cursor_factory=psycopg2.extras.DictCursor)
		psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
		cur.execute(query, bbox)
	else:
		query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}livenodes".format(config.dbtableprefix) +
			" WHERE ST_Contains(ST_GeomFromText(%s, 4326), geom);")
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
	print "num nodes", len(knownNodeIds), "in", time.time() - startTime
	queryNodes = knownNodeIds
	queryWays = []
	queryRelations = []

	CompleteQuery(conn, queryNodes, knownNodeIds, queryWays, queryRelations, enc)

	enc.Finish()
	fi.close()
	print "All done"


