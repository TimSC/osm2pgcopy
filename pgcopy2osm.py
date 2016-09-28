import csv, gzip, json, datetime
from pyo5m import o5m
import shapely.wkb as wkb

def DecodeNode(row):
	objectId, changeset, username, uid, visible, \
		timestamp, version, current, tags, gis = row[:10]

	point = wkb.loads(gis.decode("hex"))

	tagsDec = json.loads(tags.decode("UTF-8"))

	if uid == "NULL":
		uidDec = None
	else:
		uidDec = int(uid)
	if username == "NULL":
		usernameDec = None
	else:
		usernameDec = username.decode("UTF-8")

	metaData = (int(version), datetime.datetime.fromtimestamp(float(timestamp)),
		int(changeset), uidDec, usernameDec, visible=="t")

	return (int(objectId), metaData, tagsDec, (point.y, point.x))

def DecodeWay(row):
	objectId, changeset, username, uid, visible, \
		timestamp, version, current, tags, members = row[:10]

	membersDec = json.loads(members.decode("UTF-8"))
	tagsDec = json.loads(tags.decode("UTF-8"))

	if uid == "NULL":
		uidDec = None
	else:
		uidDec = int(uid)
	if username == "NULL":
		usernameDec = None
	else:
		usernameDec = username.decode("UTF-8")

	metaData = (int(version), datetime.datetime.fromtimestamp(float(timestamp)),
		int(changeset), uidDec, usernameDec, visible=="t")

	return int(objectId), metaData, tagsDec, membersDec

def DecodeRelation(row):
	objectId, changeset, username, uid, visible, \
		timestamp, version, current, tags, members, memberroles = row[:11]

	membersDec = json.loads(members.decode("UTF-8"))
	memberrolesDec = json.loads(memberroles.decode("UTF-8"))
	tagsDec = json.loads(tags.decode("UTF-8"))

	mems = []
	for (memTy, memId), memRole in zip(membersDec, memberrolesDec):
		mems.append((memTy, memId, memRole))
	if uid == "NULL":
		uidDec = None
	else:
		uidDec = int(uid)
	if username == "NULL":
		usernameDec = None
	else:
		usernameDec = username.decode("UTF-8")

	metaData = (int(version), datetime.datetime.fromtimestamp(float(timestamp)),
		int(changeset), uidDec, usernameDec, visible=="t")
	return int(objectId), metaData, tagsDec, mems

if __name__=="__main__":

	fi = gzip.open("out2.o5m.gz", "wb")
	enc = o5m.O5mEncode(fi)
	enc.StoreIsDiff(False)
	bbox = [-180.0, -90.0, 180.0, 90.0]
	enc.StoreBounds(bbox)

	#Dump nodes
	fi = csv.reader(gzip.open("/home/postgres/dumpnodes.gz", "rb"))
	count = 0
	for row in fi:
		if row[7] != "t": continue #Check if current
		if row[4] != "t": continue #Check if visible
		count += 1
		if count % 1000000 == 0:
			print count, "nodes"

		nodeData = DecodeNode(row)
		enc.StoreNode(*nodeData)

	enc.Reset()

	#Dump ways
	fi = csv.reader(gzip.open("/home/postgres/dumpways.gz", "rb"))
	count = 0
	for row in fi:
		if row[7] != "t": continue #Check if current
		if row[4] != "t": continue #Check if visible

		count += 1
		if count % 1000000 == 0:
			print count, "ways"

		wayData = DecodeWay(row)
		enc.StoreWay(*wayData)

	enc.Reset()

	#Dump relations
	fi = csv.reader(gzip.open("/home/postgres/dumprelations.gz", "rb"))
	count = 0
	for row in fi:
		if row[7] != "t": continue #Check if current
		if row[4] != "t": continue #Check if visible

		count += 1
		if count % 1000000 == 0:
			print count, "relations"

		relationData = DecodeRelation(row)
		enc.StoreRelation(*relationData)

	enc.Finish()
	fi.close()
	print "All done"
