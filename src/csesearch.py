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
import brokenlinks

# global regular expressions
reVideoLink = re.compile(videoutils.gbSettings['cse']['videoLinkExpression'])
reVideoLinkIDField = 2

# global database connection data
dbsCSEVideo = "csevideo"
dbsCSEVideoId = "_id"
dbsCSEVideoContent = "content"
dbsCSEVideoCreated = "dteCreated"

dbsStatsMapper = bson.code.Code("""
		function () {
			var thumbPattern = /.*<video\:thumbnail_loc>(.*)<\/video\:thumbnail_loc>.*/;

			var key = this._id;
			var blocks = this.content;

			var thumbsUrlHash = { };
			var contentUrlHash = { };
			blocks.forEach( function(block) {
				if (block['og:image'])
					thumbsUrlHash[ block['og:image'] ] = true;

				if (block['og:url'])
					contentUrlHash[ block['og:url'] ] = true;

				if (block['url'])
					contentUrlHash[ block['url'] ] = true;
			});

			var thumbsUrlSet = new Array();
			for (var link in thumbsUrlHash)
				thumbsUrlSet.push(link);

			var contentUrlSet = new Array();
			for (var link in contentUrlHash)
				contentUrlSet.push(link);
			
			var value = {
				csevideo: {
					status: 1,
					thumbnails: thumbsUrlSet,
					urls: contentUrlSet
				}
			};
			emit( key, value );
		}
	""")

# other global variables
fmtCSEURL = videoutils.gbSettings['cse']['cseURL']


# functions
def getVideoIdsFromCSEVideos(dbcDatabase):
	dbcCSEVideo = dbcDatabase[dbsCSEVideo]
	query = { dbsCSEVideoId: { "$ne": 0 } }
	fields = { dbsCSEVideoId: 1 }
	documents = dbcCSEVideo.find(query, fields)
	return videoutils.extractFieldFromDocuments(dbsCSEVideoId, documents)
	

def saveCSEVideo(dbcDatabase, videoId, videoContent):
	dbcCSEVideo = dbcDatabase[dbsCSEVideo]
	dteNow = datetime.datetime.utcnow()
	document = {
		dbsCSEVideoId: videoId,
		dbsCSEVideoContent: videoContent,
		dbsCSEVideoCreated: dteNow
	}

#	print "Inserting sitemap document in Mongo with:\n  video id:  {0}\n  document:  {1}".format(videoId, document)
	dbcCSEVideo.insert(document)
	return 1


def cleanCSEResult(cseDoc):
	result = { }
	for key in cseDoc:
		newKey = key.replace(".", "_")
		result[newKey] = cseDoc[key]
	return result


def getVideosFromCSEById(videoId):
	cseUrl = fmtCSEURL.format(videoId)
	session = httplib2.Http()
	response, content = session.request(cseUrl)
	if 200 == response.status:
#		print "Successfully retrieved video from cse:\n  video id:  {0}\n  http response:  {1}\n  http body:  {2}".format(videoId, response, content)
		jsonContent = json.loads(content)
		if (not jsonContent or not jsonContent['results'] or not jsonContent['results']['results'] or len(jsonContent['results']['results']) < 1 ):
			print "Failed to find any video!!!\n  Video ID: {0}\n  CSE URL: {1}\n  HTTP Response: {2}".format(videoId, cseUrl, response)
			return
		
		results = [ ]
		for result in jsonContent['results']['results']:
			link = result['url']
			if link:
				mo = reVideoLink.match(link)
				if mo:
					videoLinkId = mo.group(reVideoLinkIDField)
					if videoId == videoLinkId:
						results.append(cleanCSEResult(result))
		
		if len(results) > 0:
			return results
		else:
			print "Failed to find video items!!!\n  Video ID: {0}\n  CSE URL: {1}\n  HTTP Response: {2}".format(videoId, cseUrl, response)
			return
	else:
		print "Failed to retrieve video!!!\n  Video ID: {0}\n  CSE URL: {1}\n  HTTP Response: {2}".format(videoId, cseUrl, response)
		return


def loadCSEVideos(dbcDatabase, source, newVideoIds, maxCSERequests, delayBtwReqInSec):
	if len(newVideoIds) < 1:
		print "No videos specified to look up in cse."
		return

	print "Looking for {0} of {1} new videos in cse with {2} second delay between cse requests...".format(maxCSERequests, len(newVideoIds), delayBtwReqInSec)

	ctrRequests = 0
	ctrInserts = 0
	ctrRequestFailures = 0
	for videoId in newVideoIds:
		ctrRequests += 1
		if ctrRequests > maxCSERequests:
			break
		
		videoContent = getVideosFromCSEById(videoId)
		if videoContent:
			ctrInserts += saveCSEVideo(dbcDatabase, videoId, videoContent)
		else:
			ctrRequestFailures += 1
			brokenlinks.saveBrokenLink(dbcDatabase, videoId, source)

		sys.stdout.write('.')
		sys.stdout.flush()
		time.sleep(delayBtwReqInSec)

	print "\nProcessed cse videos:\n  videos:  {0}\n  videos succesfully added:  {1}\n  videos not available in cse:  {2}".format(len(newVideoIds), ctrInserts, ctrRequestFailures)



def loadCSEVideoFromSitemap(dbcDatabase, maxCSERequests = 100, delayBtwReqInSec = 2):
	print "Looking for videos to pull from sitemap..."
	sitemapVideoIds = sitemap.getVideoIdsFromSitemap(dbcDatabase)
	cseVideoIds = getVideoIdsFromCSEVideos(dbcDatabase)
	brokenLinkIds = brokenlinks.getVideoIdsFromBrokenLinks(dbcDatabase, 'sitemap-cse')

	newVideoIds = (sitemapVideoIds - cseVideoIds) - brokenLinkIds

	print "Found:\n  sitemap vidoes:  {0}\n  cse videos:  {1}\n  new videos to query cse for:  {2}".format(len(sitemapVideoIds), len(cseVideoIds), len(newVideoIds))
	loadCSEVideos(dbcDatabase, 'sitemap-cse', newVideoIds, maxCSERequests, delayBtwReqInSec)


# main, used for testing
def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	
	loadCSEVideoFromSitemap(dbcDatabase, 20000)

if __name__ == "__main__":
    sys.exit(main(sys.argv))