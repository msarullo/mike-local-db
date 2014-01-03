import sys
import pymongo
import datetime
import videoutils

# global source list
enumBrokenLinkSources = frozenset(["sitemap", "brokenlinks"])

# global database connection data
dbsBrokenLinks = "brokenlinks"
dbsBrokenLinksId = "_id"
dbsBrokenLinksSource = "source"
dbsBrokenLinksCreated = "dteCreated"
dbsBrokenLinksUpdated = "dteUpdated"

# functions
def getVideoIdsFromBrokenLinks(dbcDatabase, source = None):
	dbcBrokenLinks = dbcDatabase[dbsBrokenLinks]
	
	if source:
		query = { dbsBrokenLinksSource: source }
	else:
		query = { dbsBrokenLinksId: { "$ne": 0 } }

	fields = { dbsBrokenLinksId: 1 }
	documents = dbcBrokenLinks.find(query, fields)
	return videoutils.extractFieldFromDocuments(dbsBrokenLinksId, documents)


def saveBrokenLink(dbcDatabase, videoId, source):
	# validate the source
	assert source in enumBrokenLinkSources
	
	dbcBrokenLinks = dbcDatabase[dbsBrokenLinks]

	# check to see if we have a record of this id already
	query = { dbsBrokenLinksId: videoId }
	document = dbcBrokenLinks.find_one(query)
#	print "Query for broken links document by id in Mongo:\n  query:  {0}\n  result:  {1}".format(query, document)

	dteNow = datetime.datetime.utcnow()
	
	# if so, update the set of sources
	if document:
		docSources = set(document[dbsBrokenLinksSource])
		
		if source not in docSources:
			docSources.add(source)
			document[dbsBrokenLinksSource] = list(docSources)
			document[dbsBrokenLinksUpdated] = dteNow
			query = { dbsBrokenLinksId: videoId }

#			print "Updating broken links document in Mongo:\n  query:  {0}\n  document:  {1}".format(query, document)
			dbcBrokenLinks.update(query, document)
			return 2

		else:
#			print "Broken links document already contains source for video:\n  videoId:  {0}\n  source:  {1}".format(videoId, source)
			return 0

	# if not, create a record for this id
	else:
		document = {
			dbsBrokenLinksId: videoId,
			dbsBrokenLinksSource: [ source ],
			dbsBrokenLinksCreated: dteNow,
			dbsBrokenLinksUpdated: dteNow
		}

#		print "Inserting broken links document in Mongo with:\n  {0}".format(document)
		dbcBrokenLinks.insert(document)
		return 1


# main, used for testing
def main(argv):
	print "Main only used for testing!!!"
	return
	
	dbcDatabase = videoutils.connectToDatabase()
	
	print "inserts (1):  " + str(saveBrokenLink(dbcDatabase, "123", "sitemap"))
	print "skips (0):  " + str(saveBrokenLink(dbcDatabase, "123", "sitemap"))
	print "updates (2):  " + str(saveBrokenLink(dbcDatabase, "123", "brokenlinks"))

	print "fails:  " + saveBrokenLink(dbcDatabase, "123", "testbad")

if __name__ == "__main__":
    sys.exit(main(sys.argv))