from pymongo import MongoClient
from bson.son import SON
import pprint as pp
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np

client = MongoClient("mongodb://localhost:27017/")
db = client.enron_mail
messages = db.messages

lowDate = dt.datetime(1997,12,31)
highDate = dt.datetime(2004,1,1)

doc = messages.find_one()
pp.pprint(doc)

pipeline = [{"$match":{"headers.Date":{"$gt":lowDate,"$lt":highDate}}},{"$unwind":"$headers.To"},\
			{"$group":{"_id":{"to":"$headers.To","year":{"$year":"$headers.Date"},"week":{"$week":"$headers.Date"}},"cnt":{"$sum":1}}},\
			{"$sort":{"_id.to":1}},\
			{"$sort":{"cnt":-1}},\
			{"$sort":{"_id.week":1}},\
			{"$sort":{"_id.year":1}}]
			#{"$limit":500000}]

doc = messages.find_one()
pp.pprint(doc)

results = messages.aggregate(pipeline, allowDiskUse=True)

runningWeek = 0
lastWeek = 0
maxNumberOfRanks = 0
timeDictionary = dict()
currentRankList = list()
rankList = list()

for result in results:
	## Create a 2d list, row contains recipient mail id in ascending order
	weekCurrentDocument = result["_id"]["week"]
	if weekCurrentDocument != lastWeek:
		lastWeek = weekCurrentDocument
		runningWeek += 1
		print "runningWeek ", runningWeek
		## Add extra paddings of None to keep the length of each row same
		if len(currentRankList) > maxNumberOfRanks:
			maxNumberOfRanks = len(currentRankList)
			# Here I am padding extra "None" in the previous rows to make them the smae size as the new maximum rank size
			for i in range(runningWeek-1):
				rankList[i].extend(["padding"] * (maxNumberOfRanks - len(rankList[i])))
		else:
			# Here I am adding extra padding to the latest row to keep its size same as the prevoius rows.
			#print "previous rank list length: ", len(currentRankList)
			currentRankList.extend(["padding"] * (maxNumberOfRanks - len(currentRankList)))
			#print "padded rank list length: ", len(currentRankList)

		rankList.append(currentRankList)
		timeDictionary[runningWeek-1] = (result["_id"]["year"],result["_id"]["week"])## If sometime I need to retrieve the exact year and week for any index of the 2d array
		#print "timeDictionary ", timeDictionary
		currentRankList = list()
		currentRankList.append(result["_id"]["to"])
	else:
		currentRankList.append(result["_id"]["to"])
	#pp.pprint(result)
	#print "currentRankList ", len(currentRankList)

## Transpose the list
transposeRankList = map(list, zip(*rankList))
## Getting the number of rank chnages each week
numElementsinTransposeRankListNoPadding = list()
for i in range(len(transposeRankList)):
	specificRank = transposeRankList[i]
	print "specificRank ", len(specificRank)
	removedPadding = filter(lambda a: a != "padding", specificRank)
	print "removedPadding ", len(removedPadding)
	numElementsinTransposeRankListNoPadding.append(len(removedPadding))

rankChangeDistribution = list()

for i in range(len(transposeRankList)):
	setOfEmails = set(transposeRankList[i])
	#print "setOfEmails ", setOfEmails
	#print "len(setOfEmails) ", len(setOfEmails)
	setOfEmails.discard("padding")
	print len(setOfEmails)
	#print "len(setOfEmails) ",len(setOfEmails)
	rankChangeDistribution.append(len(setOfEmails)/float(numElementsinTransposeRankListNoPadding[i]))

rankChangeDistribution = np.array(rankChangeDistribution)
# rankChangeDistribution = rankChangeDistribution / float(len(rankList)) # I am dividing the number changes for each rank by the total number of time periods here.

plt.plot(range(len(rankChangeDistribution)), rankChangeDistribution, 'r--')
plt.xscale('log')
plt.show()
