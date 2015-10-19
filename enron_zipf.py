from pymongo import MongoClient
from bson.son import SON
import pprint as pp
import matplotlib.pyplot as plt
import numpy as np
from validate_email import validate_email


## Load mongodb
client = MongoClient("mongodb://localhost:27017/")
db = client.enron_mail
messages = db.messages

## Aggregate by email addresses and their occurences throughout
pipelineZipfFrom = [{"$group":{"_id":{"from":"$headers.From"},"cnt":{"$sum":1}}},\
				{"$sort":{"cnt":-1}}]

pipelineZipfTo = [{"$unwind":"$headers.To"},\
				{"$group":{"_id":{"to":"$headers.To"},"cnt":{"$sum":1}}},\
				{"$sort":{"cnt":-1}}]

currentPipeline = pipelineZipfTo
currentPipelineKey = "to"

results = messages.aggregate(currentPipeline, allowDiskUse=True)

## Store mails in an array and occurences in another array
addresses = list()
occurences = list()
for result in results:
	addresses.append(result["_id"][currentPipelineKey])
	occurences.append(result["cnt"])

## Find which mails are enron and which are non-enron
for i in range(len(addresses)):
	currentAddress = addresses[i]
	print i, currentAddress , occurences[i],
	## Remove the invalid e-mails
	if validate_email(currentAddress):
		mailDomain = currentAddress.split("@")[1].replace(".com","")
		print mailDomain 
		if  mailDomain == 'enron':
			plt.plot(i,occurences[i],'bo')
		else:
			plt.plot(i,occurences[i],'rs')
	else:
		print ""

## plot the occurences
#plt.plot(range(len(occurences)), occurences, 'r--')
plt.xscale('log')
plt.yscale('log')
plt.savefig("enron_zipf_"+currentPipelineKey+".pdf")
plt.show()