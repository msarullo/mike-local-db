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
reSitemapPubDate = re.compile('^<video:publication_date>(.*)</video:publication_date>$')
reSitemapPubDateField = 1

# global database connection data
dbsSitemap = "sitemap"
dbsSitemapId = "_id"
dbsSitemapUrls = "urls"
dbsSitemapXMLs = "xmls"
dbsSitemapLatestPubDate = "latestPubDate"
dbsSitemapCreated = "dteCreated"
dbsSitemapUpdated = "dteUpdated"

dbsStatsMapper = bson.code.Code("""
		function () {
			var key = this._id;
			var value = {
				sitemap: {
					status: 1,
					pubdate:  this.latestPubDate
				}
			};
			emit( key, value );
		}
	""")


# other global variables
stSitemapURL = videoutils.gbSettings['sitemap']['sitemapURL']
stSitemapGZFile = "sitemap.xml.gz"
docPubDateFormat = '%Y-%m-%dT%H:%M:%S'

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


def saveSitemapLink(dbcSitemap, videoId, link, pubDate, sitemapXML):
	# first, check to see if we have a record of this id already
	query = { dbsSitemapId: videoId }
	document = dbcSitemap.find_one(query)
#	print "Query for sitemap document by id in Mongo:\n  query:  {0}\n  result:  {1}".format(query, document)

	dteNow = datetime.datetime.utcnow()
	
	# if so, update the set of sitemap urls
	if document:
		sitemapUrls = set([ ])
		if dbsSitemapUrls in document:
			sitemapUrls = set(document[dbsSitemapUrls])

		sitemapXMLs = set([ ])
		if dbsSitemapXMLs in document:
			sitemapXMLs = set(document[dbsSitemapXMLs])

		oldPubDate = pubDate
		if dbsSitemapLatestPubDate in document:
			oldPubDate = document[dbsSitemapLatestPubDate]

		if link not in sitemapUrls:
			sitemapUrls.add(link)
			document[dbsSitemapUrls] = list(sitemapUrls)

			sitemapXMLs.add(sitemapXML)
			document[dbsSitemapXMLs] = list(sitemapXMLs)
			
			if pubDate > oldPubDate:
				document[dbsSitemapLatestPubDate] = pubDate

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
			dbsSitemapXMLs: [ sitemapXML ],
			dbsSitemapLatestPubDate: pubDate,
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
	ctrInvalid = 0

	state = 0
	block = ""
	link = ""
	pubDate = ""

	for line in sitemapFile:
		ctrLines += 1
		line = line.strip()
		
		# look for start of block
		if 0 == state:
			if "<url>" == line:
				state = 1
				block += line + '\n'
		
		elif 1 == state:
			
			# look for end of block
			if "</url>" == line:
				ctrLinks += 1
				block += line + '\n'
				
				# extract the video id
				videoId = getVideoIdFromLink(link)
				if videoId:
					result = saveSitemapLink(dbcSitemap, videoId, link, pubDate, block)
					if 1 == result:
						ctrInserts += 1
					elif 2 == result:
						ctrUpdates += 1
					elif 0 == result:
						ctrNoChange += 1
				else:
					print "Failed to parse video id from sitemap XML block!\n  link:  {0}\n  sitemap XML:  {1}".format(link, block)
					ctrInvalid += 1

				state = 0
				block = ""
				link = ""
				pubDate = None
			
			# capture lines in block
			else:
				block += line + '\n'
				
				# try to find the video link
 				linkMatch = reSitemapLink.match(line)
				if linkMatch:
					link = linkMatch.group(reSitemapLinkField)
					
				# try to find the pub date
				pubDateMatch = reSitemapPubDate.match(line)
				if pubDateMatch:
					pubDateStr = pubDateMatch.group(reSitemapPubDateField)
					pubDateStr = pubDateStr.split('+')[0]
					pubDate = datetime.datetime.strptime(pubDateStr, docPubDateFormat)

	sitemapFile.close()

	print "Parsed sitemap data file:\n  file:  {0}\n  lines processed:  {1}\n  links found:  {2}\n  new sitemap video:  {3}\n  added sitemap link to video:  {4}\n  unchanged links:  {5}\n  invalide sitemap entries:  {6}".format(stSitemapGZFile, ctrLines, ctrLinks, ctrInserts, ctrUpdates, ctrNoChange, ctrInvalid)


# main, used for testing
def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcSitemap = dbcDatabase[dbsSitemap]
	
	downloadSitemapVideos()
	processSitemapVideosForLinks(dbcSitemap)
	return


if __name__ == "__main__":
    sys.exit(main(sys.argv))