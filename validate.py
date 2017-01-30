from pyo5m import o5m
import gzip

class CheckUnique(object):
	def __init__(self):
		self.nodeIds = set()
		self.wayIds = set()
		self.relationIds = set()

	def StoreNode(self, objectId, metaData, tags, pos):
		if objectId in self.nodeIds:
			print "node duplicate", objectId
		self.nodeIds.add(objectId)

	def StoreWay(self, objectId, metaData, tags, refs):
		if objectId in self.wayIds:
			print "way duplicate", objectId
		self.wayIds.add(objectId)

	def StoreRelation(self, objectId, metaData, tags, refs):
		if objectId in self.relationIds:
			print "relation duplicate", objectId
		self.relationIds.add(objectId)

if __name__=="__main__":

	fi = gzip.open("uk-and-ireland-fosm-2017-01-29.o5m.gz", "rb")

	dec = o5m.O5mDecode(fi)
	checkUnique = CheckUnique()
	dec.funcStoreNode = checkUnique.StoreNode
	#dec.funcStoreWay = self.StoreWay
	#dec.funcStoreRelation = self.StoreRelation
	#dec.funcStoreBounds = self.StoreBounds
	#dec.funcStoreIsDiff = self.StoreIsDiff
	dec.DecodeHeader()

	eof = False
	while not eof:
		eof = dec.DecodeNext()

	print "All done!"
	
