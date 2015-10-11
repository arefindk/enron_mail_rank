from pymongo import MongoClient
from bson.son import SON
import pprint as pp
import datetime as dt

client = MongoClient("mongodb://localhost:27017/")
db = client.enron_mail
messages = db.messages

lowDate = dt.datetime(1998,12,31)
highDate = dt.datetime(2003,1,1)

pipeline = [{"$match":{"headers.Date":{"$gt":lowDate,"$lt":highDate}}},{"$unwind":"$headers.To"},\
			{"$group":{"_id":{"to":"$headers.To","year":{"$year":"$headers.Date"},"week":{"$week":"$headers.Date"}},"cnt":{"$sum":1}}},\
			{"$sort":{"cnt":-1}},\
			{"$sort":{"_id.week":1}},\
			{"$sort":{"_id.year":1}},\
			{"$limit":500000}]

doc = messages.find_one()

pp.pprint(doc)

results = messages.aggregate(pipeline, allowDiskUse=True)

for result in results:
	pp.pprint(result)
	# pp.pprint(result["_id"]["year"])
