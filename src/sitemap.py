import sys
import re
import pymongo
import datetime
import urllib2
import gzip
import bson

import videoutils

# global regular expressions
reVideoLink = re.compile(videoutils.gbSettings['sitemap']['videoLinkExpression'])
reVideoLinkIDField = 2
reVideoLinkSlugField = 3
reSitemapLink = re.compile('^<loc>(http://.*html)</loc>$')
reSitemapLinkField = 1


# global database connection data
dbsSitemap = "sitemap"
dbsSitemapId = "_id"
dbsSitemapUrls = "urls"
dbsSitemapCreated = "dteCreated"
dbsSitemapUpdated = "dteUpdated"

dbsStatsMapper = bson.code.Code("""
		function () {
			var key = this._id;
			var value = {
				sitemap: {
					pubdate:  this.dteCreated
				}
			};
			emit( key, value );
		}
	""")


# other global variables
stSitemapURL = videoutils.gbSettings['sitemap']['sitemapURL']
stSitemapGZFile = "sitemap.xml.gz"

# functions
def getVideoIdsFromSitemap(dbcDatabase):
	dbcSitemap = dbcDatabase[dbsSitemap]
	query = { dbsSitemapId: { "$ne": 0 } }
	fields = { dbsSitemapId: 1 }
	documents = dbcSitemap.find(query, fields)
	return videoutils.extractFieldFromDocuments(dbsSitemapId, documents)


def getVideoIdFromLink(link):
	mo = reVideoLink.match(link)
	if mo:
		videoId = mo.group(reVideoLinkIDField)
#		print "Found video id in link:\n  link:  {0}\n  video id:  {1}".format(link, videoId)
		return videoId
	else:
		print "Failed to parse video link!!!\n  Pattern: {0}\n  Video Link: {1}".format(reVideoLink.pattern, link)


def saveSitemapLink(dbcSitemap, videoId, link):
	# first, check to see if we have a record of this id already
	query = { dbsSitemapId: videoId }
	document = dbcSitemap.find_one(query)
#	print "Query for sitemap document by id in Mongo:\n  query:  {0}\n  result:  {1}".format(query, document)

	dteNow = datetime.datetime.utcnow()
	
	# if so, update the set of sitemap urls
	if document:
		sitemapUrls = set(document[dbsSitemapUrls])
		
		if link not in sitemapUrls:
			sitemapUrls.add(link)
			document[dbsSitemapUrls] = list(sitemapUrls)
			document[dbsSitemapUpdated] = dteNow
			query = { dbsSitemapId: videoId }

#			print "Updating sitemap document in Mongo:\n  query:  {0}\n  document:  {1}".format(query, document)
			dbcSitemap.update(query, document)
			return 2

		else:
#			print "Sitemap document already contains link for video:\n  videoId:  {0}\n  link:  {1}".format(videoId, link)
			return 0

	# if not, create a record for this id
	else:
		document = {
			dbsSitemapId: videoId,
			dbsSitemapUrls: [ link ],
			dbsSitemapCreated: dteNow,
			dbsSitemapUpdated: dteNow
		}

#		print "Inserting sitemap document in Mongo with:\n  {0}".format(document)
		dbcSitemap.insert(document)
		return 1


def downloadSitemapVideos():
	
	# First, download the file containing sitemap videos
	print "Downloading sitemap data:\n  url:  {0}\n  file:  {1}".format(stSitemapURL, stSitemapGZFile)
	sitemapStream = urllib2.urlopen(stSitemapURL)
	sitemapStreamSize = int(sitemapStream.info().getheaders("Content-Length")[0])

	sitemapFile = open(stSitemapGZFile, 'wb')

	bytesWritten = 0
	while True:
		streamPart = sitemapStream.read(8192)
		if not streamPart:
			break
			
		sitemapFile.write(streamPart)
		bytesWritten += len(streamPart)
		
	sitemapFile.close()
	
	if sitemapStreamSize != bytesWritten:
		print "Failed downloading sitemap data:\n  url:  {0}\n  file:  {1}\n  expected size:  {2}\n  bytes written: {3}".format(stSitemapURL, stSitemapGZFile, sitemapStreamSize, bytesWritten)
		return
		
	print "Downloaded sitemap data:\n  url:  {0}\n  file:  {1}\n  size:  {2}".format(stSitemapURL, stSitemapGZFile, bytesWritten)


def processSitemapVideosForLinks(dbcSitemap):
	print "Parsing sitemap data file:\n  file:  {0}".format(stSitemapGZFile)
	sitemapFile = gzip.open(stSitemapGZFile, 'rb')
	
	ctrLines = 0
	ctrLinks = 0
	ctrInserts = 0
	ctrUpdates = 0
	ctrNoChange = 0
	
	for line in sitemapFile:
		ctrLines += 1
		line = line.strip()

		'''
		NEED TO START CAPTURING:
		<url>
		<loc>http://www.nytimes.com/video/2014/01/03/world/africa/100000002632601/a-desperate-river-crossing.html</loc>
		<video:video>
		<video:player_loc allow_embed="no"><![CDATA[http://c.brightcove.com/services/viewer/federated_f9?&width=500&height=281&flashID=nytd_video_BrightcoveExperience&%40videoPlayer=ref%3A100000002632601&playerID=1656678357001&bgcolor=%23000000&publisherID=1749339200&isVid=true&isUI=true&wmode=transparent&dynamicStreaming=true&optimizedContentLoad=true&AllowScriptAccess=always&useExternalAdControls=true&autoStart=false&includeAPI=false&quality=high&convivaID=c3.NYTimes&convivaEnabled=true&debuggerID=&showNoContentMessage]]></video:player_loc>
		<video:thumbnail_loc>http://www.nytimes.com/images/2014/01/03/multimedia/south-sudan-refugee/south-sudan-refugee-thumbStandard.jpg</video:thumbnail_loc>
		<video:title>A Desperate River Crossing</video:title>
		<video:description>Residents of Bor, a city in the Republic of South Sudan, fled deadly violence there by taking ferries across the White Nile to Awerial, where an estimated 76,000 displaced people are stranded.</video:description>
		<video:publication_date>2014-01-04T02:36:47+00:00</video:publication_date>
		<video:duration>139</video:duration>
		<video:tag>Refugees and Displaced Persons</video:tag>
		<video:tag>Civilian Casualties</video:tag>
		<video:tag>Kulish, Nicholas</video:tag>
		<video:tag>Bor (South Sudan)</video:tag>
		<video:tag>Juba (South Sudan)</video:tag>
		<video:tag>South Sudan</video:tag>
		<video:tag>Humanitarian Aid</video:tag>
		<video:tag>Doctors Without Borders</video:tag>
		<video:category>world</video:category>
		</video:video>
		</url>
		'''
		
		# look for location fields with video links
		lineMatch = reSitemapLink.match(line)
		if lineMatch:
			ctrLinks += 1
			link = lineMatch.group(reSitemapLinkField)

			# extract the video id
			videoId = getVideoIdFromLink(link)
			
			# save the record to our database
			result = saveSitemapLink(dbcSitemap, videoId, link)
			if 1 == result:
				ctrInserts += 1
			elif 2 == result:
				ctrUpdates += 1
			elif 0 == result:
				ctrNoChange += 1
		
	sitemapFile.close()
	
	print "Parsed sitemap data file:\n  file:  {0}\n  lines processed:  {1}\n  links found:  {2}\n  new sitemap video:  {3}\n  added sitemap link to video:  {4}\n  unchanged links:  {5}".format(stSitemapGZFile, ctrLines, ctrLinks, ctrInserts, ctrUpdates, ctrNoChange)


# main, used for testing
def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcSitemap = dbcDatabase[dbsSitemap]
	
	downloadSitemapVideos()
	processSitemapVideosForLinks(dbcSitemap)
	return


if __name__ == "__main__":
    sys.exit(main(sys.argv))