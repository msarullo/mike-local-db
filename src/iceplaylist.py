import sys
import httplib2
import pymongo
import datetime
import json

import videoutils

# global database connection data
dbsIcePlaylist = "iceplaylist"
dbsIcePlaylistId = "_id"
dbsIcePlaylistContent = "content"
dbsIcePlaylistCreated = "dteCreated"
dbsIcePlaylistUpdated = "dteUpdated"

# other global variables
fmtPlaylistURL = videoutils.gbSettings['ice']['playlistURL']

# functions
def downloadICEPlaylists():
	playlistUrl = fmtPlaylistURL
	session = httplib2.Http()
	response, content = session.request(playlistUrl)
	if 200 == response.status:
#		print "Successfully retrieved playlists from ice:\n  url:  {0}\n  http response:  {1}\n  http body:  {2}".format(playlistUrl, response, content)
		jsonContent = json.loads(content)
		return jsonContent
	else:
		print "Failed to retrieve playlists!!!\n  ICE URL: {0}\n  HTTP Response: {1}".format(playlistUrl, response)


def saveICEPlaylist(dbcIcePlaylist, playlistId, playlistContent):
	# first, check to see if we have a record of this id already
	query = { dbsIcePlaylistId: playlistId }
	document = dbcIcePlaylist.find_one(query)
#	print "Query for playlist document by id in Mongo:\n  query:  {0}\n  result:  {1}".format(query, document)

	dteNow = datetime.datetime.utcnow()

	# if so, update the content
	if document:
		document[dbsIcePlaylistContent] = playlistContent
		document[dbsIcePlaylistUpdated] = dteNow
		query = { dbsIcePlaylistId: playlistId }

#		print "Updating playlist document in Mongo:\n  query:  {0}\n  document:  {1}".format(query, document)
		dbcIcePlaylist.update(query, document)
		return 2
		
	else:
		document = {
			dbsIcePlaylistId: playlistId,
			dbsIcePlaylistContent: playlistContent,
			dbsIcePlaylistCreated: dteNow,
			dbsIcePlaylistUpdated: dteNow
		}

#		print "Inserting playlist document in Mongo with:\n  playlist id:  {0}\n  document:  {1}".format(playlistId, document)
		dbcIcePlaylist.insert(document)
		return 1


def processPlaylistsFromICE(dbcIcePlaylist, playlists):
	print "Processsing {0} playlists from ICE...".format(len(playlists))
	
	ctrInserts = 0
	ctrUpdates = 0
	
	for playlistContent in playlists:
		result = saveICEPlaylist(dbcIcePlaylist, playlistContent['knewsId'], playlistContent)
		if 1 == result:
			ctrInserts += 1
		elif 2 == result:
			ctrUpdates += 1
		
	print "Processed playlist data from ICE:\n  total playlists:  {0}\n  new playlists:  {1}\n  updated playlists:  {2}".format(len(playlists), ctrInserts, ctrUpdates)


# main, used for testing
def main(argv):
	dbcDatabase = videoutils.connectToDatabase()
	dbcIcePlaylist = dbcDatabase[dbsIcePlaylist]
	
	playlists = downloadICEPlaylists()
	processPlaylistsFromICE(dbcIcePlaylist, playlists)

if __name__ == "__main__":
    sys.exit(main(sys.argv))