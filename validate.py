from pyo5m import o5m
import gzip, sys, os

#Validate an extract

class Validator(object):
	def __init__(self):
		self.nodeIds = set()
		self.wayIds = set()
		self.relationIds = set()

	def StoreNode(self, objectId, metaData, tags, pos):
		if objectId in self.nodeIds:
			print "node duplicate", objectId, "version", metaData[0]
		self.nodeIds.add(objectId)

	def StoreWay(self, objectId, metaData, tags, refs):
		if objectId in self.wayIds:
			print "way duplicate", objectId, "version", metaData[0]
		for mem in refs:
			if mem not in self.nodeIds:
				print "way", objectId, "missing node", mem

		self.wayIds.add(objectId)

	def StoreRelation(self, objectId, metaData, tags, refs):
		if objectId in self.relationIds:
			print "relation duplicate", objectId, "version", metaData[0]
		#for typeStr, refId, role in refs:
		#	if typeStr == "node" mem not in self.nodeIds:
		#		print "relation", objectId, "missing node", mem
		self.relationIds.add(objectId)

if __name__=="__main__":
	fina = "extract.o5m.gz"
	if len(sys.argv) >= 2:
		fina = sys.argv[1]

	ext = os.path.splitext(fina)[1]

	if ext != ".gz":
		fi = open(fina, "rb")
	else:
		fi = gzip.open(fina, "rb")
	
	dec = o5m.O5mDecode(fi)
	validator = Validator()
	dec.funcStoreNode = validator.StoreNode
	dec.funcStoreWay = validator.StoreWay
	dec.funcStoreRelation = validator.StoreRelation
	#dec.funcStoreBounds = self.StoreBounds
	#dec.funcStoreIsDiff = self.StoreIsDiff
	dec.DecodeHeader()

	eof = False
	while not eof:
		eof = dec.DecodeNext()

	print "All done!"
	
