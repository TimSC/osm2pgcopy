import copytodb
import psycopg2, psycopg2.extras, psycopg2.extensions
import os
import xml.etree.ElementTree as ET
import calendar
import datetime
import time
import json

if __name__=="__main__":
	config = copytodb.ReadConfig("config.cfg")
	
	conn = psycopg2.connect("dbname='{0}'".format(config["dbname"]))
	cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cur)

	prefix = config["dbtableprefix"]
	print ("Connected to", config["dbname"]+", table prefix", prefix)
	print (config["dbtablemodifyprefix"])

	sql = "DELETE FROM {0}changesets;".format(prefix)
	cur.execute(sql)

	maxIds = copytodb.GetMaxIds(cur, config, prefix)
	maxUid = maxIds["uid"]-1
	maxChangeset = maxIds["changeset"]-1

	for fi in os.listdir("changesets"):
		print fi
		tree = ET.parse(os.path.join("changesets", fi))
		for cs in tree.getroot():
			#print cs.tag
			#print cs.attrib
			objId = int(cs.attrib["id"])
			uid, user = "NULL", None
			if "uid" in cs.attrib:
				uid = int(cs.attrib["uid"])
			if "user" in cs.attrib:
				user = cs.attrib["user"]
			min_lat, min_lat, min_lon, max_lon = None, None, None, None
			try:
				rectOk = True
				if "min_lat" in cs.attrib:
					min_lat = float(cs.attrib["min_lat"])
				else:
					rectOk = False
				if "max_lat" in cs.attrib:
					max_lat = float(cs.attrib["max_lat"])
				else:
					rectOk = False
				if "min_lon" in cs.attrib:
					min_lon = float(cs.attrib["min_lon"])
				else:
					rectOk = False
				if "max_lon" in cs.attrib:
					max_lon = float(cs.attrib["max_lon"])
				else:
					rectOk = False

			except:
				rectOk = False

			is_open = cs.attrib["open"] == "true"

			tags = {}
			for ch in cs:
				if ch.tag != "tag": continue
				tags[ch.attrib["k"]] = ch.attrib["v"]

			created_at2, closed_at2 = "NULL", "NULL"
			if "created_at" in cs.attrib:
				created_at = datetime.datetime.strptime(cs.attrib["created_at"], "%Y-%m-%dT%H:%M:%SZ")
				created_at2 = int(time.mktime(created_at.timetuple()))
			if "closed_at" in cs.attrib:
				closed_at = datetime.datetime.strptime(cs.attrib["closed_at"], "%Y-%m-%dT%H:%M:%SZ")
				closed_at2 = int(time.mktime(closed_at.timetuple()))

			sql = "INSERT INTO {0}changesets(id, username, uid, tags, open_timestamp, close_timestamp, is_open".format(prefix)
			if rectOk:
				sql += ", geom"
			sql += ") VALUES ({}, %s, {}, %s, {}, {}, %s".format(objId, uid, created_at2, closed_at2)
			if rectOk:
				sql += ", ST_MakeEnvelope({}, {}, {}, {}, 4326)".format(min_lon, min_lat, max_lon, max_lat)
			sql += ");"
			cur.execute(sql, (user, json.dumps(tags), is_open))

			if uid != "NULL" and uid > maxUid:
				maxUid = uid
			if objId > maxChangeset:
				maxChangeset = objId

	sql = "DELETE FROM {0}nextids WHERE id = 'changeset';".format(prefix)
	cur.execute(sql)
	copytodb.DbExec(cur, "INSERT INTO {0}nextids (id, maxid) VALUES ('changeset', {1});".format(prefix, maxChangeset+1))

	sql = "DELETE FROM {0}nextids WHERE id = 'uid';".format(prefix)
	cur.execute(sql)
	copytodb.DbExec(cur, "INSERT INTO {0}nextids (id, maxid) VALUES ('uid', {1});".format(prefix, maxUid+1))

	conn.commit()

	copytodb.ResetChangesetUidCounts(conn, config, config["dbtableprefix"], config["dbtabletestprefix"])
	copytodb.ResetChangesetUidCounts(conn, config, config["dbtableprefix"], config["dbtablemodifyprefix"])

