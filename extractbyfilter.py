import extract, gzip, config
from pyo5m import o5m, osmxml
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2

def CheckTags(tags):
	select = False
	tags = row["tags"]
	if "natural" in tags:
	#	if tags["natural"] in ["coastline"]: select = True #"water"
		if tags["natural"] == "water" and "water" in tags and tags["water"] == "river":
			select = True
	if "waterway" in tags:
		if tags["waterway"] == "riverbank": select = True
	#if "landuse" in tags:
	#	if tags["landuse"] == "reservoir": select = True
	return select

if __name__=="__main__":
	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))

	fi = gzip.open("extractbyfilter.o5m.gz", "wb")
	enc = o5m.O5mEncode(fi)

	query = ("SELECT * FROM {0}ways".format(config.dbtableprefix) +
		" WHERE visible=true and current=true")
	cur = conn.cursor('way-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query)
	count = 0

	filteredWayIds = set()
	for row in cur:
		if CheckTags(row["tags"]):
			filteredWayIds.add(row["id"])

	cur.close()

	query = ("SELECT * FROM {0}relations".format(config.dbtableprefix) +
		" WHERE visible=true and current=true")
	cur = conn.cursor('relation-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query)
	count = 0

	filteredRelationIds = set()
	for row in cur:
		if CheckTags(row["tags"]):
			filteredRelationIds.add(row["id"])

	cur.close()

	enc.StoreIsDiff(False)

	queryNodes = []
	knownNodeIds = set()
	extract.CompleteQuery(conn, queryNodes, knownNodeIds, filteredWayIds, filteredRelationIds, enc)

	enc.Finish()
	fi.close()
	print "All done"

