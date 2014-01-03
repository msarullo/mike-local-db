import sys
import re
import pymongo
import datetime
import videoutils
import urllib2
import gzip

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