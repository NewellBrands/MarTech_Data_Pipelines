#Importing dependencies
from transfer import *
from utils import *
from listrakwriter import *
import datetime

#Setting up the proper time period for filtering data in the API
today = datetime.datetime.now()
endDate = today - datetime.timedelta(days = today.weekday()+1)
startDate = endDate - datetime.timedelta(days = 7)

startDate = startDate.strftime("%m-%d-%y")
endDate = endDate.strftime("%m-%d-%y")

#Stage I: Deliverability Report
dbParams = {'token': token, 'instance': instance, "job_id":18}
actions = [{'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['food'], 'name': 'FoodSaver','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['food'], 'name': 'FoodSaver','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUK'], 'name': 'Yankee Candle UK','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUK'], 'name': 'Yankee Candle UK','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['exofficio'], 'name': 'ExOfficio','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['exofficio'], 'name': 'ExOfficio','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUS'], 'name': 'Yankee Candle','endpoint': "messages", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUS'], 'name': 'Yankee Candle','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['food'], 'name': 'FoodSaver','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['yccUK'], 'name': 'Yankee Candle UK','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['graco'], 'name': 'Graco','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['exofficio'], 'name': 'ExOfficio','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}},
           {'brand': listDict['yccUS'], 'name': 'Yankee Candle','endpoint': "contacts", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": False, "segmentationFields":{}}]

path = "./weeklyPull/"
client = ListrakWriter(client_id, client_secret,
                        logPath = path, contactPath = path, messagePath = path,
                        summaryPath = path)

Transfer(client, dbParams).executor(actions)

#Stage II: Conversation Report
dbParams = {'token': token, 'instance': instance, "job_id":31}
actions = [{'brand': listDict['babyjogger'], 'name': 'Baby Jogger','endpoint': "conversation", "conversation": 15641, "conversationName":"Welcome Series",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "conversation", "conversation": 15639, "conversationName":"Welcome Series", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['calphalon'], 'name': 'Calphalon','endpoint': "conversation", "conversation": 16072, "conversationName":"Winback V2",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['contigo'], 'name': 'Contigo','endpoint': "conversation", "conversation": 15682, "conversationName":"Welcome Series", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['dymo'], 'name': 'DYMO','endpoint': "conversation", "conversation": 15653, "conversationName":"Welcome Series", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['exofficio'], 'name': 'ExOfficio','endpoint': "conversation", "conversation": 15589, "conversationName":"Welcome Series", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['exofficioPP'], 'name': 'ExOfficio Post Purchase','endpoint': "conversation", "conversation": 16035, "conversationName":"Product Purchase", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['foodsaver'], 'name': 'FoodSaver','endpoint': "conversation", "conversation": 15726, "conversationName":"Welcome Series",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['holmes'], 'name': 'Holmes','endpoint': "conversation", "conversation": 15724, "conversationName":"Welcome Series", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "conversation", "conversation": 15594, "conversationName":"Welcome Series", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['marmotPP'], 'name': 'Marmot Post Purchase','endpoint': "conversation", "conversation": 15955, "conversationName":"Recommendation, Nurture, and Winback", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['parker'], 'name': 'Parker','endpoint': "conversation", "conversation": 15652, "conversationName":"Welcome Series", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yankeeFrance'], 'name': 'Yankee Candle France','endpoint': "conversation", "conversation": 15617, "conversationName":"LTK - Welcome",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yankeeItaly'], 'name': 'Yankee Candle Italy','endpoint': "conversation", "conversation": 15618, "conversationName":"LTK - Welcome",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUS'], 'name': 'Yankee Candle','endpoint': "conversation", "conversation": 15615, "conversationName":"LTK - Welcome", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUK'], 'name': 'Yankee Candle UK','endpoint': "conversation", "conversation": 15616, "conversationName":"LTK - Welcome", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['babyjoggerA'], 'name': 'Baby Jogger Abandoment','endpoint': "conversation", "conversation": 15644, "conversationName":"Abandonment", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['calphalonA'], 'name': 'Calphalon Abandoment','endpoint': "conversation", "conversation": 15645, "conversationName":"Abandonment", 'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['exofficioA'], 'name': 'ExOfficio Abandoment','endpoint': "conversation", "conversation": 15633, "conversationName":"Abandonment",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['foodsaverA'], 'name': 'FoodSaver Abandoment','endpoint': "conversation", "conversation": 15742, "conversationName":"Abandonment",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccA'], 'name': 'Yankee Candle Abandoment','endpoint': "conversation", "conversation": 15838, "conversationName":"Abandonment",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['marmotA'], 'name': 'Marmot Abandoment','endpoint': "conversation", "conversation": 15634, "conversationName":"Abandonment",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yankeeUKA'], 'name': 'Yankee Candle UK Abandoment','endpoint': "conversation", "conversation": 16157, "conversationName":"Abandonment QA v2",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yankeeGermany'], 'name': 'Yankee Candle Germany','endpoint': "conversation", "conversation": 15619, "conversationName":"LTK - Welcome",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yankeeFranceA'], 'name': 'Yankee Candle France Abandoment','endpoint': "conversation", "conversation": 16000, "conversationName":"Abandonment",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           {'brand': listDict['yccUS'], 'name': 'Yankee Candle','endpoint': "conversation", "conversation": 16031, "conversationName":"Birthday Automation",'startDate': startDate, 'endDate': endDate, 'log': False, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
           ]

path = "./weeklyPull/"
client = ListrakWriter(client_id, client_secret,
                        logPath = path, contactPath = path, messagePath = path,
                        summaryPath = path)

Transfer(client, dbParams).executor(actions)
