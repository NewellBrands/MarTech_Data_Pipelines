from transfer import *
from utils import *
from listrakwriter import *
import datetime

today = datetime.datetime.now()
endDate = today - datetime.timedelta(days = today.weekday()+1)
startDate = endDate - datetime.timedelta(days = 7)

startDate = startDate.strftime("%m-%d-%y")
endDate = endDate.strftime("%m-%d-%y")

dbpath = "/Users/frank.pinto@newellco.com/Deliverability Report Automation/ETL Scripts/Weekly Email Engagements"
dbParams = {'token': token, 'dbpath' : dbpath, 'instance': instance, "job_id":18}
actions = [{'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['food'], 'name': 'Foodsaver','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['food'], 'name': 'Foodsaver','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUK'], 'name': 'YccUK','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUK'], 'name': 'YccUK','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['graco'], 'name': 'Graco','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['graco'], 'name': 'Graco','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['exofficio'], 'name': 'Exofficio','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['exofficio'], 'name': 'Exofficio','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUS'], 'name': 'YccUS','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUS'], 'name': 'YccUS','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['food'], 'name': 'Foodsaver','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['yccUK'], 'name': 'YccUK','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['graco'], 'name': 'Graco','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['exofficio'], 'name': 'Exofficio','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['yccUS'], 'name': 'YccUS','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}}]

path = "./weeklyPull/"
client = ListrakWriter(client_id, client_secret,
                        logPath = path, contactPath = path, messagePath = path,
                        summaryPath = path)

Transfer(client, dbParams).executor(actions)
