
from listrakwriter import *
from utils import *
import os
import datetime
from azure.storage.blob import ContainerClient
import csv
import ntpath
import time
from databricks_api import DatabricksAPI
import pysftp

class Transfer():

    def __init__(self, client, dbParams, upload = True, run_job = True, retries = 30, sleepTime = 3):
        self.client = client
        self.retries = retries
        self.sleepTime = sleepTime
        self.timeString = datetime.datetime.now().strftime("%m-%d-%y")
        self.dbParams = dbParams
        self.upload = upload
        self.run_job = run_job

    def downloadData(self, action):
        brand = action['brand']
        endpoint = action['endpoint']
        startDate = action['startDate']
        endDate = action['endDate']
        log = action['log']
        fileSuffix = action['fileSuffix']
        uploadPath = action['uploadPath']
        destination = action['destination']
        if destination == 'sftp':
            self.sftp_creds = action['creds']

        self.client.setList(self.brand, self.name)

        if self.endpoint == "messages":
            try:
                filesToDownload = self.client.allMessages(startDate, endDate, log, fileSuffix)
            except RuntimeError as runErr:
                raise RuntimeError("No Files To Download.")
            else:
                filesToDownload = ["{}_message_{}.csv".format(self.brand, val) for val in filesToDownload]
                logData = [[str(self.brand), file, "PENDING DOWNLOAD", self.client.messagePath, str(destination), str(uploadPath)] for file in filesToDownload]
                self.writeLog(logData, update = False)
                filesDownloaded = self.client.getMessages(startDate = startDate, endDate = endDate,
                                                    log = log, fileSuffix = fileSuffix)
        elif self.endpoint == "summary":
            #WRITE LOG
            fileName = "{}_summary_".format(self.client.listId)
            if not startDate:
                startDate = "09-01-2018"
            if not endDate:
                endDate = datetime.datetime.now().strftime("%m-%d-%Y")
            fileName = fileName + "{}_{}".format(startDate, endDate)
            if fileSuffix:
                fileName = fileName + "_{}".format(fileSuffix)
            fileName = fileName + ".csv"
            logData = [[str(brand), fileName, "PENDING DOWNLOAD", self.client.messagePath, str(destination), str(uploadPath)]]
            self.writeLog(logData, update = False)
            print("Fetching data for the summary of the week starting on {} thru {}".format(startDate, endDate))
            filesDownloaded = self.client.getSummary(startDate = startDate, endDate = endDate,
                                                log = log, fileSuffix = fileSuffix)

        elif self.endpoint == "contacts":
            if not action['jsonOutput']:
                fileName = str(brand) + "_contacts"
                if fileSuffix:
                    fileName = fileName + "_{}".format(fileSuffix)
                fileName = fileName + ".csv"
                logData = [[str(brand), fileName, "PENDING DOWNLOAD", self.client.messagePath, str(destination), str(uploadPath)]]
                self.writeLog(logData, update = False)
            print("Fetching contact for brand {}".format(brand))
            filesDownloaded = self.client.getContacts(startDate = startDate, endDate = endDate,
                                                      log = log, fileSuffix = fileSuffix,
                                                      subscribed = action['subscribed'], segmentationFields = action['segmentationFields'], jsonOutput = action['jsonOutput'])
            logData = [[str(self.brand), ntpath.basename(file), "PENDING DOWNLOAD", self.client.messagePath, str(destination), str(uploadPath)] for file in filesDownloaded]
            self.writeLog(logData, update = False)

        elif self.endpoint == 'conversation':
            filesDownloaded = self.client.getConversationMessages(action['conversation'], action['conversationName'],startDate = startDate, endDate = endDate,
                                                                  log = log, fileSuffix = fileSuffix)
            logData = [[str(self.brand), ntpath.basename(file), "PENDING DOWNLOAD", self.client.messagePath, str(destination), str(uploadPath)] for file in filesDownloaded]
            self.writeLog(logData, update = False)

        #UPDATE LOG
        if not filesDownloaded:
            raise RuntimeError("No Files were downloaded.")
        logData = [ntpath.basename(file) for file in filesDownloaded]
        pipelineLog = self.writeLog(logData, status = "FILE DOWNLOADED")
        return pipelineLog

    def writeLog(self, data, status = None, update = True):
        pipelineLog = "pipelineLog/pipelineLog_{}_{}_{}.txt".format(self.brand, self.endpoint, self.timeString)
        if not update:
            with open(pipelineLog, "w") as f:
                for logItem in data:
                    f.write("|".join(logItem))
                    f.write("\n")
        else:
            with open(pipelineLog, "r") as f:
                oldData = f.read().splitlines()
            newData = self.updateData(oldData, data, status)
            with open(pipelineLog, "w") as f:
                for logItem in newData:
                    f.write("|".join(logItem))
                    f.write("\n")
        return pipelineLog

    def updateData(self, oldData, newData, status):
        output = []
        for logItem in oldData:
            line = logItem.split("|")
            if line[1] in newData:
                output.append([line[0], line[1], status, line[3], line[4], line[5]])
            else:
                output.append(line)
        return output

    def uploadData(self, pipelineLog):
        with open(pipelineLog, 'r') as f:
            logData = f.read().splitlines()
        filesUploaded = []
        for logItem in logData:
            row = logItem.split("|")
            if row[-2] == 'datalake':
                containerClient = ContainerClient.from_connection_string(storageCreds, 'datalake')
                fileName = row[1]
                localPath = self.client.messagePath + fileName
                uploadPath = row[-1] + fileName
                with open(localPath, "rb") as f:
                    print("Sending File: {}".format(fileName))
                    try:
                        containerClient.upload_blob(uploadPath, f, overwrite = True)
                    except:
                        print("File {} was not uploaded".format(row[2]))
                filesUploaded.append(fileName)
                os.remove(localPath)
            elif row[-2] == 'sftp':
                fileName = row[1]
                localPath = self.client.messagePath + fileName
                uploadPath = row[-1] + fileName
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                try:
                    with pysftp.Connection(self.sftp_creds['url'], username=self.sftp_creds['user'], password=self.sftp_creds['password'], cnopts = cnopts) as con:
                        con.put(localPath,uploadPath)
                except:
                    print("File {} was not uploaded".format(row[2]))
                filesUploaded.append(fileName)
                os.remove(localPath)
        self.writeLog(filesUploaded, status = "FILE UPLOADED")

    def logConsolidator(self):
        fileList = [fileName for fileName in os.listdir("pipelinelog") if ("txt" in fileName) and (self.timeString in fileName)]
        for file in fileList:
            with open("./pipelinelog/" + file, "r") as f:
                data = f.read().splitlines()
            with open("./pipelinelog/consolidatedLog_{}.txt".format(self.timeString), "a+", newline = "") as f:
                for line in data:
                    f.write(line)
                    f.write("\n")

            os.remove("./pipelinelog/" + file)

    def jobRunner(self):
        client = DatabricksAPI(host = self.dbParams['instance'], token = self.dbParams['token'])
        resp = client.jobs.run_now(job_id = self.dbParams['job_id'])
        return resp


    def executor(self, actions):
        """
        actions: [{brand: 'brand', endpoint: 'endpoint', startDate: 'startDate',
                   endDate: 'endDate', log: 'log', fileSuffix: 'fileSuffix', destination: 'destination', uploadPath: 'uploadPath'},
                  ...]
        """
        for action in actions:
            self.brand = action['brand']
            self.name = action['name']
            self.endpoint = action['endpoint']
            print("Working with data for brand {} and endpoint {}".format(self.brand, self.endpoint))
            #DOWNLOAD DATA
            try:
                dataUpload = self.downloadData(action)
            except RuntimeError as runErr:
                print(runErr)
            else:
                #UPLOAD DATA
                if self.upload:
                    self.uploadData(dataUpload)
                else:
                    pass
            print()
        print("Consolidating Log")
        print()
        self.logConsolidator()
        if self.run_job:
            print("Tasking Databrick Cluster")
            self.jobRunner()


if __name__ == "__main__":
    startDate = "04-05-2020"
    endDate = "04-12-2020"
    path = "./test/"
    client = ListrakWriter(client_id, client_secret,
                                logPath = path, contactPath = path, messagePath = path,
                                summaryPath = path)

    dbParams = {'token': token, 'instance': instance, "job_id": 18}
    actions = [#{'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "messages", 'startDate': startDate, 'endDate': None, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/", "subscribed": True, "segmentationFields": {}},
               #{'brand': listDict['marmot'], 'name': 'Marmot','endpoint': "summary", 'startDate': startDate, 'endDate': endDate, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrivals/"},
               {'brand': listDict['yccUS'], 'name': 'YccUS','endpoint': "contacts", 'startDate': None, 'endDate': None, 'log': True, 'subscribed': True, 'segmentationFields':{},'fileSuffix': 'test', 'destination' : 'sftp', 'creds':{'url': 'mctc659s6fvgc5-vg35769jt9gsm.ftp.marketingcloudops.com', 'user':'110006176', 'password': '4KT.AN!@e!23hN!1'},'uploadPath': "/Export/reports/", 'jsonOutput':True},
               #{'brand': listDict['food'], 'name': 'FoodSaver','endpoint': "messages", 'startDate': startDate, 'endDate': None, 'log': True, 'fileSuffix': 'test', 'destination' : 'sftp', 'creds':{'url': 'mctc659s6fvgc5-vg35769jt9gsm.ftp.marketingcloudops.com', 'user':'110006176', 'password': '4KT.AN!@e!23hN!1'},'uploadPath': "/Export/reports/"},
               #{'brand': listDict['calphalon'], 'endpoint': "messages", 'startDate': startDate, 'endDate': None, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrival/"},
               #{'brand': listDict['calphalon'], 'endpoint': "summary", 'startDate': startDate, 'endDate': None, 'log': True, 'fileSuffix': None, 'destination' : 'datalake', 'uploadPath': "arrival/"},
               ]
    Transfer(client, dbParams, upload = True, run_job = False).executor(actions)
    #client.setList(listDict['marmot'])
    #print(client.getSummary(startDate = startDate))
