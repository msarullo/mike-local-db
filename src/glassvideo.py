import sys
import re
import httplib2
import pymongo
import datetime
import time
import json
import bson

import videoutils
import sitemap
import sitesearch
import brokenlinks


# global regular expressions
reVideoLink = re.compile(videoutils.gbSettings['glass']['videoLinkExpression'])
reVideoLinkIDField = 2
reVideoLinkSlugField = 3

# global database connection data
dbsGlassVideo = "glassvideo"
dbsGlassVideoId = "_id"
dbsGlassVideoContent = "content"
dbsGlassVideoCreated = "dteCreated"
dbsGlassVideoUpdated = "dteUpdated"
dbsGlassVideoContentPubDate = "content.cms.video.publication_dt"

dbsStatsMapper = bson.code.Code("""
		function () {
			var timebits = /^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]{3})\.([A-Z]{3})$/;
		    var m = timebits.exec(this.content.cms.video.publication_dt);
		    var resultDateStr;
		    if (m) {
		    	resultDateStr = m[2]+'-'+m[3]+'-'+m[1]+' '+m[4]+':'+m[5]+':'+m[6]+' '+m[8];
		    }
		
			var key = this._id;
			var value = {
				glassvideo: {
					status: 1,
					pubdate:  new Date(resultDateStr),
				}
			};
			emit( key, value );
		}
	""")
#	pubdate_str: resultDateStr,
#	pubdate_org: this.content.cms.video.publication_dt

dbsPlaylistMapper = bson.code.Code("""
		function() {
			var playlists = this.content.cms.video.playlists;
			var vId = this._id;
			playlists.forEach( function(playlist) {
				var value = {
					glassvideo: [ vId ]
				};
				emit( playlist.id.value, value )
			});
		}
	""")


# other global variables
fmtGlassURL = videoutils.gbSettings['glass']['glassURL']

# functions
def getVideoIdsFromGlassVideos(dbcDatabase):
	dbcGlassVideo = dbcDatabase[dbsGlassVideo]
	query = { dbsGlassVideoId: { "$ne": 0 } }
	fields = { dbsGlassVideoId: 1 }
	documents = dbcGlassVideo.find(query, fields)
	return videoutils.extractFieldFromDocuments(dbsGlassVideoId, documents)


def getVideoFromGlassById(videoId):
	glassUrl = fmtGlassURL.format(videoId)
	session = httplib2.Http()
	response, content = session.request(glassUrl)
	if 200 == response.status:
#		print "Successfully retrieved video from glass:\n  video id:  {0}\n  http response:  {1}\n  http body:  {2}".format(videoId, response, content)
		jsonContent = json.loads(content)
		return jsonContent
	else:
		print "Failed to retrieve video!!!\n  Video ID: {0}\n  Glass URL: {1}\n  HTTP Response: {2}".format(videoId, glassUrl, response)


def saveGlassVideo(dbcGlassVideo, videoId, videoContent):
	dteNow = datetime.datetime.utcnow()
	document = {
		dbsGlassVideoId: videoId,
		dbsGlassVideoContent: videoContent,
		dbsGlassVideoCreated: dteNow,
		dbsGlassVideoUpdated: dteNow
	}

#	print "Inserting sitemap document in Mongo with:\n  video id:  {0}\n  document:  {1}".format(videoId, document)
	dbcGlassVideo.insert(document)
	return 1


def loadGlassVideos(dbcDatabase, dbcGlassVideo, source, newVideoIds, maxGlassRequests, delayBtwReqInSec):
	if len(newVideoIds) < 1:
		print "No videos specified to look up in glass."
		return

	print "Looking for {0} of {1} new videos in glass with {2} second delay between glass requests...".format(maxGlassRequests, len(newVideoIds), delayBtwReqInSec)

	ctrRequests = 0
	ctrInserts = 0
	ctrRequestFailures = 0
	for videoId in newVideoIds:
		ctrRequests += 1
		if ctrRequests > maxGlassRequests:
			break
		
		videoContent = getVideoFromGlassById(videoId)
		if videoContent:
			ctrInserts += saveGlassVideo(dbcGlassVideo, videoId, videoContent)
		else:
			ctrRequestFailures += 1
			brokenlinks.saveBrokenLink(dbcDatabase, videoId, source)

		sys.stdout.write('.')
		sys.stdout.flush()
		time.sleep(delayBtwReqInSec)

	print "\nProcessed glass videos:\n  videos:  {0}\n  videos succesfully added:  {1}\n  videos not available in glass:  {2}".format(len(newVideoIds), ctrInserts, ctrRequestFailures)


def loadGlassVideoFromSitemap(dbcDatabase, dbcGlassVideo, maxGlassRequests = 100, delayBtwReqInSec = 2):
	print "Looking for videos to pull from sitemap..."
	sitemapVideoIds = sitemap.getVideoIdsFromSitemap(dbcDatabase)
	glassVideoIds = getVideoIdsFromGlassVideos(dbcDatabase)
	brokenLinkIds = brokenlinks.getVideoIdsFromBrokenLinks(dbcDatabase, 'sitemap')

	newVideoIds = (sitemapVideoIds - glassVideoIds) - brokenLinkIds

	print "Found:\n  sitemap vidoes:  {0}\n  glass videos:  {1}\n  broken sitemap videos:  {2}\n  new videos to query glass for:  {3}".format(len(sitemapVideoIds), len(glassVideoIds), len(brokenLinkIds), len(newVideoIds))
	loadGlassVideos(dbcDatabase, dbcGlassVideo, 'sitemap', newVideoIds, maxGlassRequests, delayBtwReqInSec)


def loadGlassVideoFromBrokenLinks(dbcDatabase, dbcGlassVideo, maxGlassRequests = 100, delayBtwReqInSec = 2):
	print "Looking for videos to pull from broken links..."
	glassVideoIds = getVideoIdsFromGlassVideos(dbcDatabase)
	brokenLinkIds = brokenlinks.getVideoIdsFromBrokenLinks(dbcDatabase)

	newVideoIds = brokenLinkIds - glassVideoIds

	print "Found:\n  broken link vidoes:  {0}\n  glass videos:  {1}\n  new videos to query glass for:  {2}".format(len(brokenLinkIds), len(glassVideoIds), len(newVideoIds))
	loadGlassVideos(dbcDatabase, dbcGlassVideo, 'brokenlinks', newVideoIds, maxGlassRequests, delayBtwReqInSec)


def loadGlassVideoFromSiteSearch(dbcDatabase, dbcGlassVideo, maxGlassRequests = 100, delayBtwReqInSec = 2):
	print "Looking for videos to pull from site search..."
	sitesearchIds = sitesearch.getVideoIdsFromSiteSearch(dbcDatabase)
	glassVideoIds = getVideoIdsFromGlassVideos(dbcDatabase)
	brokenLinkIds = brokenlinks.getVideoIdsFromBrokenLinks(dbcDatabase, 'sitesearch')

	newVideoIds = (sitesearchIds - glassVideoIds) - brokenLinkIds

	print "Found:\n  sitesearch vidoes:  {0}\n  glass videos:  {1}\n  broken sitesearch videos:  {2}\n  new videos to query glass for:  {3}".format(len(sitesearchIds), len(glassVideoIds), len(brokenLinkIds), len(newVideoIds))
	loadGlassVideos(dbcDatabase, dbcGlassVideo, 'sitesearch', newVideoIds, maxGlassRequests, delayBtwReqInSec)


# main, used for testing
def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcGlassVideo = dbcDatabase[dbsGlassVideo]
	
	loadGlassVideoFromSitemap(dbcDatabase, dbcGlassVideo, 100)
	loadGlassVideoFromSiteSearch(dbcDatabase, dbcGlassVideo, 100)
#	loadGlassVideoFromBrokenLinks(dbcDatabase, dbcGlassVideo, 100, 2)

if __name__ == "__main__":
    sys.exit(main(sys.argv))