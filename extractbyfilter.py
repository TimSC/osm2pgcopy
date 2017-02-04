import extract, gzip, config
from pyo5m import o5m, osmxml
import psycopg2, psycopg2.extras, psycopg2.extensions #apt install python-psycopg2

if __name__=="__main__":
	conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(config.dbname, config.dbuser, config.dbhost, config.dbpass))

	fi = gzip.open("extractbyfilter.o5m.gz", "wb")
	enc = o5m.O5mEncode(fi)

	enc.StoreIsDiff(False)

	queryNodes = [35308285, 301253]
	knownNodeIds = set()
	queryWays = [32207303]
	queryRelations = [1894868]

	extract.CompleteQuery(conn, queryNodes, knownNodeIds, queryWays, queryRelations, enc)

	enc.Finish()
	fi.close()
	print "All done"

