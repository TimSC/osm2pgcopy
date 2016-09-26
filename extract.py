from pyo5m import osmxml
import gzip, json, config, array
import psycopg2, psycopg2.extras #apt install python-psycopg2

def GetWaysForNodes(conn, qids, nodeIds, extraNodeIds, knownWayIds):
	sqlFrags = []
	wayTable = "{0}ways".format(config.dbtableprefix)
	wayMemTable = "{0}way_mems".format(config.dbtableprefix)
	for qid in qids:
		sqlFrags.append("{0}.member = %s".format(wayMemTable))

	sql = "SELECT {0}.* FROM {1} INNER JOIN {0} ON {1}.id = {0}.id AND {1}.version = {0}.version WHERE current = true and visible = true AND ({2});".format(wayTable, wayMemTable, " OR ".join(sqlFrags))

	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
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
	
	return wcount
	
def GetNodesById(conn, qids):
	sqlFrags = []
	for qid in qids:
		sqlFrags.append("id = %s")
	query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}nodes".format(config.dbtableprefix) +
		" WHERE ({0}) and visible=true and current=true;".format(" OR ".join(sqlFrags)))
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(query, qids)
	count = 0

	for row in cur:
		count += 1

def GetRelationsForObjects(conn, qtype, qids, knownRelationIds, relationIdsOut):
	#print qids
	sqlFrags = []
	relTable = "{0}relations".format(config.dbtableprefix)
	relMemTable = "{0}relation_mems_{1}".format(config.dbtableprefix, qtype)
	for qid in qids:
		sqlFrags.append("{0}.member = %s".format(relMemTable))
	sql = "SELECT {0}.* FROM {1} INNER JOIN {0} ON {1}.id = {0}.id AND {1}.version = {0}.version WHERE current = true and visible = true AND ({2});".format(relTable, relMemTable, " OR ".join(sqlFrags));

	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(sql, qids)
	count = 0
	for row in cur:
		count += 1
		rid = row["id"]
		if rid not in knownRelationIds:
			relationIdsOut.add(rid)

if __name__=="__main__":
	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))
	bbox = [20.8434677,39.6559274,20.8699036,39.6752201] #Town in greece

	#Get nodes within bbox
	knownNodeIds = set()

	query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}nodes".format(config.dbtableprefix) +
		" WHERE geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326) " +
		" and visible=true and current=true;")
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute(query, bbox)
	for row in cur:
		knownNodeIds.add(row["id"])
		#TODO write to output

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
	cursor = 0
	for qid in extraNodeIds:
		qids.append(qid)
		if len(qids) >= step:
			GetNodesById(conn, qids)
			qids = []
	if len(qids) > 0:
		GetNodesById(conn, qids)

	knownNodeIds.update(extraNodeIds)
	del extraNodeIds

	print "nodes to complete ways done"

	#Get relations for these objects
	cursor = 0
	knownRelationIds = set()
	for qid in knownNodeIds:
		qids.append(qid)
		if len(qids) >= step:
			GetRelationsForObjects(conn, "n", qids, knownRelationIds, knownRelationIds)
			qids = []
	if len(qids) > 0:
		GetRelationsForObjects(conn, "n", qids, knownRelationIds, knownRelationIds)
	print "num relations from nodes", len(knownRelationIds)
	
	cursor = 0
	relationsFromWays = set()
	for qid in knownWayIds:
		qids.append(qid)
		if len(qids) >= step:
			GetRelationsForObjects(conn, "w", qids, knownRelationIds, relationsFromWays)
			qids = []
	if len(qids) > 0:
		GetRelationsForObjects(conn, "w", qids, knownRelationIds, relationsFromWays)
	print "num relations from ways", len(relationsFromWays)
	knownRelationIds.update(relationsFromWays)

	#Collect relations of relations
	seekingRelIds = knownRelationIds
	for i in range(10):
		cursor = 0
		extraRelationIds = set()
		for qid in seekingRelIds:
			qids.append(qid)
			if len(qids) >= step:
				GetRelationsForObjects(conn, "r", qids, knownRelationIds, extraRelationIds)
				qids = []
		if len(qids) > 0:
			GetRelationsForObjects(conn, "r", qids, knownRelationIds, extraRelationIds)
		print "extraRelationIds", len(extraRelationIds)
		
		knownRelationIds.update(extraRelationIds)
		if len(extraRelationIds) == 0:
			break
		seekingRelIds = extraRelationIds

	print "All done"


