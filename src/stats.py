import sys
import pymongo
import bson

import videoutils
import sitemap
import glassvideo
import sitesearch

# report on refers???

#  database specific global variables
dbsCollections = sorted([ sitemap.dbsSitemap, glassvideo.dbsGlassVideo, sitesearch.dbsSitesearch ])
dbsCollectionMappers = {
	sitemap.dbsSitemap: sitemap.dbsStatsMapper,
	glassvideo.dbsGlassVideo: glassvideo.dbsStatsMapper,
	sitesearch.dbsSitesearch: sitesearch.dbsStatsMapper
}
dbsCollectionSetsForComparison = {
	"Site map videos in Glass": [ sitemap.dbsSitemap, glassvideo.dbsGlassVideo ],
	"Site search videos in Glass": [ sitesearch.dbsSitesearch, glassvideo.dbsGlassVideo ],
	"Glass videos in site map": [ glassvideo.dbsGlassVideo, sitemap.dbsSitemap ],
	"Glass videos in site search": [ glassvideo.dbsGlassVideo, sitesearch.dbsSitesearch ]
}

dbsVideoStats = "videostats"
dbsVideoStatsPubDate = "pubdate"


# functions
def analyzeVideoCollections(dbcDatabase):
	dbcVideoStats = dbcDatabase[dbsVideoStats]
	dbcVideoStats.remove()
	
	for collectionName in dbsCollectionMappers.keys():
		print "Analyzing {0} collection...".format(collectionName)
		
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


def printTotals(dbcVideoStats):
	print "\nCollection Name, Total Videos, Oldest Pub Date, Newest Pub Date"

	for collection in dbsCollections:
		result = dbcVideoStats.aggregate([
				{ "$match": { "value.{0}.status".format(collection): 1 } },
				{ "$sort": { "value.{0}.pubdate".format(collection): 1 } },
 				{ "$group": {
					"_id": "videocollections",
					"count": { "$sum": "$value.{0}.status".format(collection) },
					"oldest": { "$first": "$value.{0}.pubdate".format(collection) },
					"newest": { "$last": "$value.{0}.pubdate".format(collection) }
				} }
			])

		result = result['result'][0]
		print ','.join([ collection, str(result['count']), str(result['oldest']), str(result['newest']) ])


def printSetComparisons(dbcVideoStats):
	print "\nComparison, Total Videos in Set, Overlapping Videos, Difference"
	for comparisonName in sorted(dbsCollectionSetsForComparison.keys()):
		seta, setb = dbsCollectionSetsForComparison[comparisonName]
		result = dbcVideoStats.aggregate([
				{ "$match": { "value.{0}.status".format(seta): 1 } },
				{ "$group": {
					"_id": "sets",
					"counta": { "$sum": "$value.{0}.status".format(seta) },
					"countb": { "$sum": "$value.{0}.status".format(setb) },
				} }
			])

		result = result['result'][0]
		print ','.join([ comparisonName, str(result['counta']), str(result['countb']), str(result['counta'] - result['countb']) ])


def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcVideoStats = dbcDatabase[dbsVideoStats]

	analyzeVideoCollections(dbcDatabase)

	printTotals(dbcVideoStats)
	printSetComparisons(dbcVideoStats)

if __name__ == "__main__":
    sys.exit(main(sys.argv))