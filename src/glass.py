import sys
import re
import httplib2
import pymongo
import datetime
import time
import json

import videoutils
import sitemap
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


# main, used for testing
def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcGlassVideo = dbcDatabase[dbsGlassVideo]
	
#	loadGlassVideoFromBrokenLinks(dbcDatabase, dbcGlassVideo, 1000, 2)
	loadGlassVideoFromSitemap(dbcDatabase, dbcGlassVideo, 2, 2)


if __name__ == "__main__":
    sys.exit(main(sys.argv))