from pyo5m import o5m
import gzip, json, config, datetime
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2

if __name__=="__main__":
	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))
	#left,bottom,right,top
	bbox = [-180.0, -90.0, 180.0, 90.0]

	fi = gzip.open("out.o5m.gz", "wb")
	enc = o5m.O5mEncode(fi)
	enc.StoreIsDiff(False)
	enc.StoreBounds(bbox)

	#Get nodes
	count = 0
	query = ("SELECT *, ST_X(geom) as lon, ST_Y(geom) AS lat FROM {0}nodes".format(config.dbtableprefix) +
		" WHERE visible=true and current=true;")
	cur = conn.cursor('node-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query, bbox)
	for row in cur:
		count+= 1
		if count % 1000000 == 0:
			print count, "nodes"
		nid = row["id"]
		metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
			row["changeset"], row["uid"], row["username"], row["visible"])
		enc.StoreNode(nid, metaData, row["tags"], (row["lat"], row["lon"]))

	cur.close()
	print "num nodes", count
	enc.Reset()

	#Get ways
	query = ("SELECT * FROM {0}ways".format(config.dbtableprefix) +
		" WHERE visible=true and current=true;")
	cur = conn.cursor('way-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query)
	count = 0

	for row in cur:
		count += 1
		if count % 1000000 == 0:
			print count, "ways"
		wid = row["id"]
		metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
			row["changeset"], row["uid"], row["username"], row["visible"])

		enc.StoreWay(wid, metaData, row["tags"], row["members"])

	cur.close()
	print "num ways", count
	enc.Reset()

	#Get relations
	query = ("SELECT * FROM {0}relations".format(config.dbtableprefix) +
		" WHERE visible=true and current=true;")
	cur = conn.cursor('relation-cursor', cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)
	cur.execute(query)
	count = 0

	for row in cur:
		count += 1
		if count % 1000000 == 0:
			print count, "relations"
		wid = row["id"]
		mems = []
		for (memTy, memId), memRole in zip(row["members"], row["memberroles"]):
			mems.append((memTy, memId, memRole))
		
		metaData = (row["version"], datetime.datetime.fromtimestamp(row["timestamp"]),
			row["changeset"], row["uid"], row["username"], row["visible"])
		enc.StoreWay(wid, metaData, row["tags"], mems)

	cur.close()
	print "num relatons", count

	enc.Finish()
	fi.close()
	print "All done"


