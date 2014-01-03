import sys
import pymongo
import json

# global dynamic settings
gbSettingsFileName = "video-settings.json"
global gbSettings
gbSettings = None

# global database connection data
dbsName = "videos"


# main, used for testing
def connectToDatabase():
	print "Connecting to MongoDB video database..."
	dbcClient = pymongo.MongoClient()
	dbcDatabase = dbcClient[dbsName]
	print "MongoDB video database connected."
	return dbcDatabase


def extractFieldFromDocuments(field, documents):
	fieldList = set([ ])
	for document in documents:
		fieldList.add(document[field])
	return fieldList


def main(argv):
	print "Main only used for testing!!!"
	return


if __name__ == "__main__":
    sys.exit(main(sys.argv))

# load the settings file if we have not already done so
if not gbSettings:
	print "Loading global video settings from {0}".format(gbSettingsFileName)
	settingsFile = open(gbSettingsFileName, 'rt')
	gbSettings = json.load(settingsFile)
	settingsFile.close()
