# enron_mail_rank

Download the dateset from this link and then untar it:
[Enron email dataset](http://www.cs.cmu.edu/~enron/)

### Now use the python script to put all the data inside mongodb, given mongodb is already installed and running in port 27017
```
python enronMaildir2Mongo.py ~/Downloads/maildir/
```

Change the maildirectory to your download directory.

### Now convert all the date fields from string to actual ISODate using mongo shell:
```
use enron_mail

db.messages.find().forEach(function(doc) { doc.headers.Date = new Date(Date.parse(doc.headers.Date.toString())); db.messages.save(doc); })
```

