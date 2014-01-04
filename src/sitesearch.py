import sys
import re
import pymongo
import datetime
import httplib2
import json
import calendar
import time

import videoutils

# global regular expressions
reVideoLink = re.compile(videoutils.gbSettings['sitesearch']['videoLinkExpression'])
reVideoLinkIDField = 2
reVideoLinkSlugField = 3

# global database connection data
dbsSitesearch = "sitesearch"
dbsSitesearchId = "_id"
dbsSitesearchDoc = "doc"
dbsSitesearchCreated = "dteCreated"
dbsSitesearchUpdated = "dteUpdated"

# other global variables
fmtSearchURL = videoutils.gbSettings['sitesearch']['searchURL']
docWebURL = 'web_url'
docPubDate = 'pub_date'
docPubDateFormat = '%Y-%m-%dT%H:%M:%SZ'

# functions
def getVideoIdFromSearchDoc(searchDoc):
	link = searchDoc[docWebURL]
	mo = reVideoLink.match(link)
	if mo:
		videoId = mo.group(reVideoLinkIDField)
#		print "Found video id in link:\n  link:  {0}\n  video id:  {1}".format(link, videoId)
		return videoId
	else:
		print "Failed to parse video link!!!\n  Pattern: {0}\n  Video Link: {1}".format(reVideoLink.pattern, link)


def saveSearchDoc(dbcSitesearch, searchDoc):
	# determine video id from search doc
	videoId = getVideoIdFromSearchDoc(searchDoc)
	if not videoId:
		return -1

	# check to see if we have a record of this id already
	query = { dbsSitesearchId: videoId }
	document = dbcSitesearch.find_one(query)
#	print "Query for sitemap document by id in Mongo:\n  query:  {0}\n  result:  {1}".format(query, document)

	dteNow = datetime.datetime.utcnow()

	# already have some version of this document
	if document:
		
		# check the pub dates		
		localPubDate = datetime.datetime.strptime(searchDoc[docPubDate], docPubDateFormat)	
		dbPubDate = datetime.datetime.strptime(document[dbsSitesearchDoc][docPubDate], docPubDateFormat)

		# db has the same pub date, skip it
		if localPubDate <= dbPubDate:
#			print "Sitesearch document in db already:\n  videoId:  {0}".format(videoId)
			return 0
		
		# already have it but the pub date is newer, update it
		document[dbsSitesearchDoc] = searchDoc
		document[dbsSitesearchUpdated] = dteNow
		query = { dbsSitesearchId: videoId }

#		print "Updating sitesearch document in Mongo:\n  query:  {0}\n  document:  {1}".format(query, document)
		dbcSitesearch.update(query, document)
		return 2
	
	# insert it
	document = {
		dbsSitesearchId: videoId,
		dbsSitesearchDoc: searchDoc,
		dbsSitesearchCreated: dteNow,
		dbsSitesearchUpdated: dteNow
	}

#	print "Inserting sitesearch document in Mongo with:\n  {0}".format(document)
	dbcSitesearch.insert(document)
	return 1


def searchVideoInDayPage(dayYYYYMMDD, page):
#	print "Searching for video:\n  day:  {0}\n  page:  {1}".format(dayYYYYMMDD, page)

	searchUrl = fmtSearchURL.format(dayYYYYMMDD, page)
	session = httplib2.Http()

	response, content = session.request(searchUrl)
	if 200 == response.status:
		jsonContent = json.loads(content)
		docs = jsonContent['response']['docs']
#		print "Successfully searched for video from site search:\n  day:  {0}\n  page:  {1}\n  docs found:  {2}\n  http response:  {3}\n  http body:  {4}".format(dayYYYYMMDD, page, len(docs), response, content)
		return docs
	else:
		print "Failed to search video!!!\n  day:  {0}\n  page:  {1}\n  search URL: {2}\n  HTTP Response: {3}".format(dayYYYYMMDD, page, searchUrl, response)


def getSearchDocsInDay(dbcSitesearch, dayYYYYMMDD, showSummary = True, delayBtwPagesInSec = 1):
	sys.stdout.write("Searching for video in day {0}.".format(dayYYYYMMDD))
	sys.stdout.flush()

	ctrInserts = 0
	ctrUpdates = 0
	ctrNoChange = 0
	ctrNotVideo = 0

	page = 0
	while True:
		searchDocs = searchVideoInDayPage(dayYYYYMMDD, page)
#		print "\nFound:\n  day:  {0}\n  page:  {1}\n  docs:  {2}\n".format(dayYYYYMMDD, page, len(searchDocs))
		if len(searchDocs) < 1:
			break;

		for searchDoc in searchDocs:
			result = saveSearchDoc(dbcSitesearch, searchDoc)
			if 1 == result:
				ctrInserts += 1
			elif 2 == result:
				ctrUpdates += 1
			elif 0 == result:
				ctrNoChange += 1
			elif result < 0:
				ctrNotVideo += 1

		time.sleep(delayBtwPagesInSec)
		sys.stdout.write('.')
		sys.stdout.flush()
		page += 1

	if showSummary:
		print "\nParsed search results:\n  day:  {0}\n  pages:  {1}\n  new sitesearch videos:  {2}\n  updated sitesearch videos:  {3}\n  unchanged sitesearch videos:  {4}\n  invalid sitesearch videos:  {5}".format(dayYYYYMMDD, page, ctrInserts, ctrUpdates, ctrNoChange, ctrNotVideo)
	else:
		sys.stdout.write(" found {0}/{1}/{2} new/updated/invalid videos.\n".format(ctrInserts, ctrUpdates, ctrNotVideo))
		sys.stdout.flush()

	return page, ctrInserts, ctrUpdates, ctrNoChange, ctrNotVideo
	

def getSearchDocsInMonth(dbcSitesearch, year, month, delayBtwDaysInSec = 2):
	firstWeekDay, lastDay = calendar.monthrange(year, month)
	firstDay = 1
	print "Searching for video from {0}-{1:02d}-{2:02d} to {0}-{1:02d}-{3:02d}...".format(year, month, firstDay, lastDay)

	ctrPages = 0
	ctrInserts = 0
	ctrUpdates = 0
	ctrNoChange = 0
	ctrNotVideo = 0

	for day in range(firstDay, lastDay + 1):
		dayPages, dayInserts, dayUpdates, dayNoChange, dayNotVideo = getSearchDocsInDay(dbcSitesearch, "{0}{1:02d}{2:02d}".format(year, month, day), False)
		ctrPages += dayPages
		ctrInserts += dayInserts
		ctrUpdates += dayUpdates
		ctrNoChange += dayNoChange
		ctrNotVideo += dayNotVideo
		time.sleep(delayBtwDaysInSec)

	print "Parsed search results:\n  pages:  {0}\n  new sitesearch videos:  {1}\n  updated sitesearch videos:  {2}\n  unchanged sitesearch videos:  {3}\n  invalid sitesearch videos:  {4}".format(ctrPages, ctrInserts, ctrUpdates, ctrNoChange, ctrNotVideo)



# main, used for testing
def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcSitesearch = dbcDatabase[dbsSitesearch]

#	getSearchDocsInMonth(dbcSitesearch, 2013, 12)
#	getSearchDocsInMonth(dbcSitesearch, 2013, 11)
#	getSearchDocsInMonth(dbcSitesearch, 2013, 10)
#	getSearchDocsInMonth(dbcSitesearch, 2013, 9)
#	getSearchDocsInMonth(dbcSitesearch, 2013, 8)
#	getSearchDocsInMonth(dbcSitesearch, 2013, 7)
	getSearchDocsInMonth(dbcSitesearch, 2013, 6)
	getSearchDocsInMonth(dbcSitesearch, 2013, 5)
	getSearchDocsInMonth(dbcSitesearch, 2013, 4)
	getSearchDocsInMonth(dbcSitesearch, 2013, 3)
	getSearchDocsInMonth(dbcSitesearch, 2013, 2)
	getSearchDocsInMonth(dbcSitesearch, 2013, 1)

	getSearchDocsInMonth(dbcSitesearch, 2012, 12)
	getSearchDocsInMonth(dbcSitesearch, 2012, 11)
	getSearchDocsInMonth(dbcSitesearch, 2012, 10)
	getSearchDocsInMonth(dbcSitesearch, 2012, 9)
	getSearchDocsInMonth(dbcSitesearch, 2012, 8)
	getSearchDocsInMonth(dbcSitesearch, 2012, 7)
	getSearchDocsInMonth(dbcSitesearch, 2012, 6)
	getSearchDocsInMonth(dbcSitesearch, 2012, 5)
	getSearchDocsInMonth(dbcSitesearch, 2012, 4)
	getSearchDocsInMonth(dbcSitesearch, 2012, 3)
	getSearchDocsInMonth(dbcSitesearch, 2012, 2)
	getSearchDocsInMonth(dbcSitesearch, 2012, 1)

#	getSearchDocsInDay(dbcSitesearch, "20131231")
#	getSearchDocsInDay(dbcSitesearch, "20130626", False)
	return


if __name__ == "__main__":
    sys.exit(main(sys.argv))