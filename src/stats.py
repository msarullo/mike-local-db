import sys
import pymongo
import bson
import json

import videoutils
import sitemap
import glassvideo
import sitesearch
import iceplaylist
import csesearch

# report on refers???

#  database specific global variables
dbsCollections = sorted([ sitemap.dbsSitemap, glassvideo.dbsGlassVideo, sitesearch.dbsSitesearch ])
dbsCollectionMappers = {
	sitemap.dbsSitemap: sitemap.dbsStatsMapper,
	glassvideo.dbsGlassVideo: glassvideo.dbsStatsMapper,
	sitesearch.dbsSitesearch: sitesearch.dbsStatsMapper,
	csesearch.dbsCSEVideo: csesearch.dbsStatsMapper
}
dbsPlaylistMappers = {
	glassvideo.dbsGlassVideo: glassvideo.dbsPlaylistMapper,
#	sitesearch.dbsSitesearch: sitesearch.dbsPlaylistMapper
}
dbsCollectionSetsForComparison = {
	"Site map videos in Glass": [ sitemap.dbsSitemap, glassvideo.dbsGlassVideo ],
	"Site search videos in Glass": [ sitesearch.dbsSitesearch, glassvideo.dbsGlassVideo ],
	"Glass videos in site map": [ glassvideo.dbsGlassVideo, sitemap.dbsSitemap ],
	"Glass videos in site search": [ glassvideo.dbsGlassVideo, sitesearch.dbsSitesearch ]
}

dbsVideoStats = "statsvideo"
dbsVideoStatsPubDate = "pubdate"

dbsPlaylistVideos = "statsplaylistvideo"

dbsUrlStats = "statsurl"


# functions
def analyzeVideoCollections(dbcDatabase):
	dbcVideoStats = dbcDatabase[dbsVideoStats]
	dbcVideoStats.remove()
	
	for collectionName in dbsCollectionMappers.keys():
		print "Analyzing videos in collection:  {0}".format(collectionName)
		
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
		
		dbcDatabase[collectionName].map_reduce(mapFunc, reduceFunc, { 'reduce': dbsVideoStats })


def analyzePlaylistsVideos(dbcDatabase):
	dbcPlaylistVideos = dbcDatabase[dbsPlaylistVideos]
	dbcPlaylistVideos.remove()

	for collectionName in dbsPlaylistMappers.keys():
		print "Analyzing playlists in collection:  {0}".format(collectionName)
		
		# execute a map-reduce on this collection into video-stats
		mapFunc = dbsPlaylistMappers[collectionName]

		reduceFunc = bson.code.Code("""
				function(key, values) {
					var reducedObject = { """+collectionName+""": [ ] };
				
					values.forEach( function(value) {
						value."""+collectionName+""".forEach( function(videoId) {
							reducedObject."""+collectionName+""".push(videoId);
						});
					});
				
					return reducedObject;
				}
			""")
		
		dbcDatabase[collectionName].map_reduce(mapFunc, reduceFunc, { 'reduce': dbsPlaylistVideos })
	

def printTotals(dbcVideoStats):
	print "\nCollection Name, Total Videos, Oldest Pub Date, Newest Pub Date"

	for collection in dbsCollections:
		result = dbcVideoStats.aggregate([
				{ "$match": { "value.{0}.status".format(collection): 1 } },
 				{ "$group": {
					"_id": "videocollections",
					"count": { "$sum": "$value.{0}.status".format(collection) },
					"oldest": { "$min": "$value.{0}.pubdate".format(collection) },
					"newest": { "$max": "$value.{0}.pubdate".format(collection) }
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


def analyzeSitemapUrlStats(dbcDatabase):
	dbcDatabase[dbsUrlStats].remove()

	mapFunc = bson.code.Code("""
			function () {
				var urlPattern = /^http:\/\/([a-zA-Z]+)\.nytimes\.com\/(.*)\/(.*)\.html$/;
				var datePattern = /.*\/(20[01][0-9])\/([01][0-9])\/([0-3][0-9])\/.*/;
				var playerPattern = /.*<video\:player_loc( allow_embed="(yes|no)")?>(.*)<\/video\:player_loc>.*/;
				var thumbPattern = /.*<video\:thumbnail_loc>(.*)<\/video\:thumbnail_loc>.*/;
				var pubDatePattern = /.*<video\:publication_date>(20[01][0-9])-([01][0-9])-([0-3][0-9])T.*<\/video\:publication_date>.*/;
				
				var videoId = this._id;
				var urls = this.urls;
				var xmls = this.xmls;
				for (var idx = 0; idx < urls.length; idx++) {
					var url = urls[idx];
					var sitemapXML = xmls[idx];

					var sitemapTagsHash = { }
					var starts = sitemapXML.split("<");
					starts.forEach( function(start) {
						if ('/' == start.charAt(0)) {
							var tag = start.split(">", 1);
							sitemapTagsHash[tag[0].substr(1)] = true;
						}
					});
					
					var sitemapTagsSet = new Array();
					for (var tag in sitemapTagsHash)
						sitemapTagsSet.push(tag);
					
					var playerMatch = playerPattern.exec(sitemapXML);
					var thumbMatch = thumbPattern.exec(sitemapXML);
					var pubDateMatch = pubDatePattern.exec(sitemapXML);
					var urlMatch = urlPattern.exec(url);
					var dateMatch = datePattern.exec(url);
					
					var normalizedPath;
					if (urlMatch) {
						normalizedPath = urlMatch[2].replace(videoId, "")
						if (dateMatch) {
							datePath = '/' + dateMatch[1] + '/' + dateMatch[2] + '/' + dateMatch[3]
							normalizedPath = normalizedPath.replace(datePath, "")
						}
					}

					var value = {
						'videoId': videoId,
						'url': url,
						'urlMiss': (! urlMatch),
						'urlPartSubDomain': (urlMatch ? urlMatch[1] : ''),
						'urlPartPath': (urlMatch ? urlMatch[2] : ''),
						'urlPartSlug': (urlMatch ? urlMatch[3] : ''),
						'urlPartNormalizedPath': normalizedPath,
						'dateMiss': (! dateMatch),
						'datePartYear': (dateMatch ? dateMatch[1] : ''),
						'datePartMonth': (dateMatch ? dateMatch[2] : ''),
						'datePartDay': (dateMatch ? dateMatch[3] : ''),
						'pubDateYear': (pubDateMatch ? pubDateMatch[1] : ''),
						'pubDateMonth': (pubDateMatch ? pubDateMatch[2] : ''),
						'pubDateDay': (pubDateMatch ? pubDateMatch[3] : ''),
						'sitemapTags': sitemapTagsSet,
						'playerLoc': (playerMatch ? playerMatch[3] : ''),
						'playerEmbed': (playerMatch ? playerMatch[2] : ''),
						'thumbLoc': (thumbMatch ? thumbMatch[1] : '')
					};
					
					emit( url, value );
				};
			}
		""")

	reduceFunc = bson.code.Code("""
			function(key, values) {
				return values;
			}
		""")
	
	dbcDatabase[sitemap.dbsSitemap].map_reduce(mapFunc, reduceFunc, { 'reduce': dbsUrlStats })
	

def printSitemapDailyStats(dbcDatabase):
	dbcUrlStats = dbcDatabase[dbsUrlStats]

	result = dbcUrlStats.aggregate([
			{ "$project": {
				"_id": 1,
				"value": 1,
				"multimedia": { "$cond": [ { "$strcasecmp": ["$value.urlPartNormalizedPath", "video/multimedia/"] }, 0, 1 ] }
			} },
			{ "$group": {
				"_id": { "$concat": ["$value.pubDateYear", "$value.pubDateMonth", "$value.pubDateDay"] },
				"multimedia": { "$sum": "$multimedia" },
				"count": { "$sum": 1 }
			} }
		])

	print "Date ID,Year,Month,Day,Multimedia,Total"
	for dayStats in result['result']:
		dateId = dayStats["_id"]
		print ",".join([ dateId, dateId[0:4], dateId[4:6], dateId[6:], str(dayStats["multimedia"]), str(dayStats["count"]) ])


def printSitemapUrlStats(dbcDatabase):
	dbcUrlStats = dbcDatabase[dbsUrlStats]
	urlStats = { }
	urlStats['totalUrls'] = dbcUrlStats.count()

	result = dbcUrlStats.aggregate([
			{ "$group": {
				"_id": "$value.urlMiss",
				"count": { "$sum": 1 },
			} }
		])
	urlStats['invalidVideoUrls'] = result['result']

	result = dbcUrlStats.aggregate([
			{ "$group": {
				"_id": "$value.urlPartSubDomain",
				"count": { "$sum": 1 },
			} }
		])
	urlStats['urlSubdomains'] = result['result']
	
	result = dbcUrlStats.aggregate([
			{ "$group": {
				"_id": "$value.urlPartNormalizedPath",
				"count": { "$sum": 1 },
			} }
		])
	urlStats['urlPaths'] = result['result']
	
	result = dbcUrlStats.aggregate([
			{ "$group": {
				"_id": "$value.datePartYear",
				"count": { "$sum": 1 },
			} }
		])
	urlStats['urlYears'] = result['result']

	result = dbcUrlStats.aggregate([
			{ "$group": {
				"_id": "$value.videoId",
				"count": { "$sum": 1 },
			} },
			{ "$group": {
				"_id": "$count",
				"numberOfVideosAtThisRate": { "$sum": 1 },
			} },
		])
	urlStats['freqencyOfUrlsPerVideoId'] = result['result']

	result = dbcUrlStats.aggregate([
			{ "$group": {
				"_id": "$value.sitemapTags",
				"count": { "$sum": 1 },
			} },
		])
	urlStats['sitemapEntryProperties'] = result['result']

	print json.dumps(urlStats, sort_keys=True, indent=4)
	

def printPlaylistInfo(dbcPlaylistVideos):
	print "\nPlaylist Data:Set,Min Videos,Max Videos, Avg Videos"
	print "FINISH ME! probably need a map reduce to count the size of the glass video array!"


def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcVideoStats = dbcDatabase[dbsVideoStats]
	dbcPlaylistVideos = dbcDatabase[dbsPlaylistVideos]

#	analyzeVideoCollections(dbcDatabase)
#	analyzePlaylistsVideos(dbcDatabase)
#	analyzeSitemapUrlStats(dbcDatabase)

	printTotals(dbcVideoStats)
#	printSetComparisons(dbcVideoStats)
#	printPlaylistInfo(dbcPlaylistVideos)
#	printSitemapUrlStats(dbcDatabase)
#	printSitemapDailyStats(dbcDatabase)


# look for sitemap urls in cse:
#db.statsvideo.aggregate(
#{ "$unwind": "$value.sitemap.urls" },
#{ "$match": { "value.csevideo.status": 1 } },
#{ "$unwind": "$value.csevideo.urls" },
#{ "$project": { "sitemapUrl": "$value.sitemap.urls", "cseUrl": "$value.csevideo.urls", "same": { "$cond": [ { "$strcasecmp": [ "$value.sitemap.urls", "$value.csevideo.urls"] }, 0, 1 ] } } },
#{ "$group": { "_id": "sitemapUrlsInCSE", "pairs": { "$sum": 1 }, "matches": { "$sum": "$same" } } }) 


if __name__ == "__main__":
    sys.exit(main(sys.argv))