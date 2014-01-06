import sys
import pymongo
import bson

import videoutils
import sitemap
import glass
import sitesearch

#  database specific global variables
dbsCollections = [ sitemap.dbsSitemap, glass.dbsGlassVideo, sitesearch.dbsSitesearch ]
dbsCollectionPubDateFields = {
#	sitemap.dbsSitemap: sitemap.dbsSitemap,
	glass.dbsGlassVideo: glass.dbsGlassVideoContentPubDate,
	sitesearch.dbsSitesearch: sitesearch.dbsSitesearchDocPubDate
}
dbsCollectionMappers = {
	sitemap.dbsSitemap: sitemap.dbsStatsMapper,
	glass.dbsGlassVideo: glass.dbsStatsMapper,
	sitesearch.dbsSitesearch: sitesearch.dbsStatsMapper
}

dbsVideoStats = "videostats"
dbsVideoStatsPubDate = "pubdate"


# functions
def helperFlattenDict(dd, separator='.', prefix=''):
    return { prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in helperFlattenDict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }


def getTotalCount(dbcDatabase, collection):
	dbcCollection = dbcDatabase[collection]
	return dbcCollection.count()
	

def findVideoField(dbcDatabase, collection, videoField, fieldSort = None):
	dbcCollection = dbcDatabase[collection]
	resultFields = { videoField: 1 }
	if fieldSort:
		resultSort = [ ( videoField, fieldSort ) ]
	else:
		resultSort = None

	cursor = dbcCollection.find(fields=resultFields, sort=resultSort)
	if cursor and cursor.count() > 0:
		result = helperFlattenDict(cursor[1])
		return result[videoField]

	return "QUERY FAILED"


def getOldestPubDate(dbcDatabase, collection, pubDateField):
	return findVideoField(dbcDatabase, collection, pubDateField, 1)


def getNewestPubDate(dbcDatabase, collection, pubDateField):
	return findVideoField(dbcDatabase, collection, pubDateField, -1)


def printTotals(dbcDatabase):
	data = { }	
	for collection in dbsCollections:
		collectionData = [ collection ]
		collectionData.append(str(getTotalCount(dbcDatabase, collection)))

		if collection in dbsCollectionPubDateFields:
			pubDateField = dbsCollectionPubDateFields[collection]
			collectionData.append(getOldestPubDate(dbcDatabase, collection, pubDateField))
			collectionData.append(getNewestPubDate(dbcDatabase, collection, pubDateField))
		else:
			collectionData.append('UNKNOWN')
			collectionData.append('UNKNOWN')

		data[collection] = collectionData

	print "Collection Name, Total Videos, Oldest Pub Date, Newest Pub Date"
	for collection in sorted(data.keys()):
		collectionData = data[collection]
		print ','.join(collectionData)


def analyzeVideoCollections(dbcDatabase):
	dbcVideoStats = dbcDatabase[dbsVideoStats]
	dbcVideoStats.remove()
	
	for collectionName in dbsCollectionMappers.keys():
		print "Analyzing collection {0}...".format(collectionName)
		
		# execute a map-reduce on this collection into video-stats
		mapFunc = dbsCollectionMappers[collectionName]

		reduceFunc = bson.code.Code("""
				function(key, values) {
					var reducedObject = { };
				
					values.forEach( function(value) {
						for (collection in value) {
							reducedObject[collection] = value[collection];
						}
					});
				
					return reducedObject;
				}
			""")
		
		dbcDatabase[collectionName].map_reduce(mapFunc, reduceFunc, { 'reduce': dbsVideoStats})


def printSetComparisons(dbcDatabase):
	analyzeVideoCollections(dbcDatabase)


def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
#	printTotals(dbcDatabase)
	printSetComparisons(dbcDatabase)

if __name__ == "__main__":
    sys.exit(main(sys.argv))