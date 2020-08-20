from utils import *
import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import datetime
import pandas as pd
import csv
import time
import os
import json


class BearerAuth(requests.auth.AuthBase):
    token = None
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class ListrakWriter():
    """
    The ListrakClient object is meant to be used to make data pulls from the Listrak API. It requires a client_id and client_secret
    for a specific IP address previously whitelisted.

    Args:
        client_id (str): api client unique identifier.
        client_secret (str): api secret.
        list_id (int): list identifier.
        max_attempts (int): max amount of tries the code will attempt a request should an error arise.
        retry_delay (int): amount of seconds it will wait in between attempts when an error arises.
        activityOutput (bool): a flag to signal if the object should output an activity log.
        logPath (str): the relative path where to save the log files.
        contactPath (str): the relative path where to save the contact files.
        messagePath (str): the relative path where to save the message files.
        summaryPath (str): the relative path where to save the summary files.

    Attributes:
        list_id (int): list identifier.
        max_attempts (int): max amount of tries the code will attempt a request should an error arise.
        retry_delay (int): amount of seconds it will wait in between attempts when an error arises.
        activityOutput (bool): a flag to signal if the object should output and activity log.
        logPath (str): the path where to save the log files.
        baseUrl (str): the base url from which requests will be built from in other modules. If no list_id is provided
                       when object is initiated, then it will only give access to general summary data of all lists
                       tied to credentials.
        token (str): the api credential that must be used in all requests in order to successfully query the data.
        expirationI (datetime): a timestamp that indicates the validity of the token. Once expired, a new token must be
                                obtained.

    """

    def __init__(self, client_id, client_secret, listId = None, listName = None, max_attempts = 20, retry_delay = 3, log = False, logPath = None, contactPath = None, messagePath = None, summaryPath = None):
        self.listId = listId
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.log = log
        self.contactPath = contactPath
        self.messagePath = messagePath
        self.summaryPath = summaryPath
        self.listName = listName
        self.logFileName = "activitylog_{}.txt".format(datetime.datetime.now().strftime("%m%d%y"))
        if logPath:
            self.logFileName = logPath + self.logFileName
        if listId:
            self.baseUrl = "https://api.listrak.com/email/v1/List/{}/".format(self.listId)
        else:
            self.baseUrl = "https://api.listrak.com/email/v1/List"
        self.token, self.expirationT = self.auth(client_id, client_secret)

    def auth(self, client_id, client_secret):
        """
        The auth method takes the client_id and client_secret strings when the object is initiated
        and authenticates the user. It returns a string token and datetime object marking the validity
        of it.

        Args:
            client_id (str): api client unique identifier.
            client_secret (str): api secret.
        """
        token_url = "https://auth.listrak.com/OAuth2/Token"
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(token_url=token_url, client_id=client_id, client_secret=client_secret, include_client_id = True)
        return token['access_token'], datetime.datetime.now() + datetime.timedelta(seconds = int(token['expires_in']))

    def getLists(self):
        """
        The getList method returns a list of all lists associated to the credentials passed when the ListrakClient
        was initiated, if no specific listId is specified. Else, it returns a dictionary object with summary data about
        the list_id originally passed.
        """
        if self.expirationT <= datetime.datetime.now():
            self.token, self.expirationT = self.auth(client_id, client_secret)
        else:
            pass
        return requests.get(self.baseUrl, auth = BearerAuth(self.token)).json()['data']

    def setList(self, listId, listName = None ):
        """
        The setList method is used to change the list that will be queried inside of Listrak by updating the baseUrl attribute.

        Args:
            listId (int): list identifier.
        """
        self.baseUrl = "https://api.listrak.com/email/v1/List/{}/".format(listId)
        self.listId = listId
        self.listName = listName

    def getRequest(self, endpoint, log, max_attempts=20, retry_delay=3):
        """
        The getRequest method makes the request to the corresponding endpoint defined in the parameters.

        Args:
            endpoint (str): the endpoint that will be used in the API call.
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
            max_attempts (int): maximun amount of times the code will retry a request if it were to run into
                                an error.
            retry_delay (int): amount of seconds the code will wait in between attempts.
        """
        url = self.baseUrl + endpoint
        if (self.max_attempts != 20) or (self.retry_delay != 3):
            max_attempts = self.max_attempts
            retry_delay = self.retry_delay
        attempts = 0
        while True:
            if self.expirationT <= datetime.datetime.now():
                self.token, self.expirationT = self.auth(client_id, client_secret)
            try:
                response = requests.get(url, auth=BearerAuth(self.token))
                response.raise_for_status()
                if log:
                    success = ['success', datetime.datetime.now().strftime("%m/%d/%Y %H:%M"),  str(self.listId), url, "-"]
                    self.logWriter(success)
                return response
            except requests.exceptions.RequestException as reqError:
                attempts += 1
                time.sleep(retry_delay)
                if attempts < max_attempts:
                    if log:
                        failure = ["failure", datetime.datetime.now().strftime("%m/%d/%Y %H:%M"), str(self.listId), url, str(reqError.response)]
                        self.logWriter(failure)
                else:
                    print(reqError.response)
                    if log:
                        failure = ["catastrophic_failure", datetime.datetime.now().strftime("%m/%d/%Y %H:%M"), str(self.listId), url, str(reqError.response)]
                        self.logWriter(failure)
                    raise RuntimeError("Max number of retries met.")

    def isResponseEmpty(self, data, url, log, shutdown = True):
        """
        This method checks if the value for the 'data' key in the response obtained
        back from Listrak is empty. It can raise an exception, or print an error message.

        Arg:
            data (list): response obtained from Listrak.
            url (string): url to be tried again in the case the response is empty.
            shutdown (bool): flag used to indicate when the code should raise an exception
                             or just print and error message.
        """
        if not data['data']:
            for _ in range(self.max_attempts):
                time.sleep(self.retry_delay)
                try:
                    data = self.getRequest(url, log).json()
                except RuntimeError as runError:
                    if shutdown:
                        raise runError
                    else:
                        print('Max number of retries met.')
                except KeyboardInterrupt as key:
                    raise key
                else:
                    if not data['data']:
                        if _ == self.max_attempts - 1:
                            if log:
                                failure = ["catastrophic_failure", datetime.datetime.now().strftime("%m/%d/%Y %H:%M"), str(self.listId), url, "Empty Listrak Response.", str(data)]
                                self.logWriter(failure)
                            if shutdown:
                                raise RuntimeError("Empty Listrak Response.")
                            else:
                                print("Empty Listrak Response.")
                    else:
                        return data['data']
        else:
            return data['data']

    def specificMessage(self, messageId, log, fileSuffix):
        """
        The specificMessage method handles all requests related to specific messages, through the unique message identifier.

        Args:
            messageId (int): the Listrak uniquer identifier for messages
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
            fileSuffix (string): a suffix to be added to the end of the file name
        """
        messageUrl = "Message/{}".format(messageId)

        try:
            responseMessage = self.getRequest(messageUrl, log).json()
        except RuntimeError as runError:
            raise runError
        except KeyboardInterrupt as key:
            raise key
        else:
            responseMessage = self.isResponseEmpty(responseMessage, messageUrl, log)

        subject = responseMessage['subject']

        url = messageUrl + "/Activity?count=5000"
        counter = 0
        nextPage = True
        while nextPage:
            if counter == 0:
                nurl = url
            else:
                nurl = url + "&cursor={}".format(nextPage)
            try:
                response = self.getRequest(nurl, log).json()
            except RuntimeError as runError:
                raise runError
            except KeyboardInterrupt as key:
                raise key
            else:
                data = self.isResponseEmpty(response, nurl, log)
                self.messageOutput(data, messageId, subject, fileSuffix)
                nextPage = response['nextPageCursor']
                counter += 1
        if log:
            success = ["success", datetime.datetime.now().strftime("%m/%d/%Y %H:%M"), str(self.listId),
                       "message file created: {}".format(messageId), "-"]
            self.logWriter(success)

    def messageOutput(self, data, messageId, subject, fileSuffix):
        """
        The messageOutput method handles writes csv files for each message sought after by the specificMessage method.

        Args:
            data (list): list comprised of the summary activity for each email that received the message.
            messageId (int): the Listrak uniquer identifier for messages.
            subject (string): subject line of the message.
            fileSuffix (string): a suffix to be added to the end of the file name.
            counter (int): counter of how many batches have been written to the csv file.
        """
        fileName = str(self.listId) + "_message_" + str(messageId)
        if fileSuffix:
            fileName = fileName + "_{}".format(fileSuffix)
        if self.messagePath:
            fileName = self.messagePath + fileName
        fileName = fileName + ".csv"
        self.fileName = fileName

        colNames = ['abuse', 'activityDate', 'bounce', 'bounceReason', 'click', 'clickCount', 'emailAddress',
                    'open', 'orderTotal', 'read', 'sendDate', 'unsubscribe', 'visitDate', 'externalContactID']
        with open(fileName, mode = 'a+', encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            if os.stat(fileName).st_size == 0:
                #writer.writerow(['brand'] + ['messageId'] + ['subject'] + [key for key, val in data[0].items()])
                writer.writerow(['brand'] + ['messageId'] + ['subject'] + [key for key in colNames])
            if self.listName:
                name = self.listName
            else:
                name = self.listId
            [writer.writerow([name] + [messageId] + [subject] + [dat[col] for col in colNames]) for dat in data]

    def allMessages(self, startDate, endDate, log, fileSuffix):
        '''
        The allMessages method builds a list of message id for those that were sent out in a given date range. If no
        startDate or endDate are provided, the method will pull data for all messages in the list.

        Args:
            startDate (string): date string to filter out the messages that were sent before it. Has the format of mm-dd-yyyy.
            endDate (string): date string to filter out the messages that were sent after it. Has the format of mm-dd-yyyy.
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
            fileSuffix (string): a suffix to be added to the end of the file name
        '''
        if (startDate) and (endDate):
            dateRangeUrl = "Message?startDate={}&endDate={}".format(startDate, endDate)
        elif startDate:
            dateRangeUrl = "Message?startDate={}".format(startDate)
        elif endDate:
            dateRangeUrl = "Message?endDate={}".format(endDate)
        else:
            dateRangeUrl = "Message"
        try:
            response = self.getRequest(dateRangeUrl, log).json()
        except RuntimeError as runError:
            raise runError
        except KeyboardInterrupt as key:
            raise key
        else:
            data = self.isResponseEmpty(response, dateRangeUrl, log)
            return [val['messageId'] for val in data]

    def getMessages(self, messageId = None, startDate = None, endDate = None, log = False, fileSuffix = None):
        '''
        The getMessages method is a wrapper built around specificMessage and allMessages methods that handles all message
        data related requests. If no messageId, startDate or endDate are provided to the method, then it will pull all email
        engagement data for all messages.

        Args:
            messageId (int): the Listrak uniquer identifier for messages
            startDate (string): date string to filter out the messages that were sent before it. Has the format of mm-dd-yyyy.
            endDate (string): date string to filter out the messages that were sent after it. Has the format of mm-dd-yyyy.
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
            fileSuffix (string): a suffix to be added to the end of the file name
        '''
        if self.log:
            log = True
        if messageId:
            print("Currently fetching data for Message {}".format(messageId))
            try:
                self.specificMessage(messageId, log, fileSuffix)
            except RuntimeError as runError:
                print("Max number of retries met. Message {} has been skipped".format(messageId))
            except KeyboardInterrupt as key:
                print("Operation Canceled")
            else:
                return [self.fileName]
        elif (any(val is not None for val in [startDate, endDate])) or (all(val is None for val in [startDate, endDate])):
            try:
                messages = self.allMessages(startDate, endDate, log, fileSuffix)
            except RuntimeError as runError:
                print("Max number of retries met. Message has been skipped")
                pass
            except KeyboardInterrupt as key:
                print("Operation Canceled")
            else:
                fileList = []
                for val in messages:
                    print("Currently fetching data for Message {}".format(val))
                    try:
                        self.specificMessage(val, log, fileSuffix)
                    except RuntimeError as runError:
                        print("Max number of retries met. Message has been skipped")
                        pass
                    except KeyboardInterrupt as key:
                        print("Operation Canceled")
                        break
                    else:
                        fileList.append(self.fileName)
                return fileList

    def listrakSummary(self, messageId, log):
        """
        Fetches summarized data for each message passed as a parameter from the Listrak Message Summary endpoint.

        Args:
            messageId (int): the Listrak uniquer identifier for messages
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
        """
        url = "Message/{}/Summary".format(messageId)

        try:
            responseSummary = self.getRequest(url, log).json()
        except RuntimeError as runError:
            raise runError
        except KeyboardInterrupt as key:
            raise key
        else:
            data = self.isResponseEmpty(responseSummary, url, log)
            keys = ['averageOrderValue', 'bounceCount', 'clickCount', 'conversionCount', 'openCount', 'passAlongCount', 'readCount', 'revenue', 'sentCount', 'unsubscribeCount', 'visitCount']
            return [data[key] for key in keys]

    def getSummary(self, messageId = None, startDate = None, endDate = None, log = False, fileSuffix = None):
        '''
        The getSummary method fetches Listrak's message summary and its calculate deliverability metrics. If no messageId, startDate or endDate are
        provided to the method, then it will pull data for all messages.

        Args:
            messageId (int): the Listrak uniquer identifier for messages
            startDate (string): date string to filter out the messages that were sent before it. Has the format of mm-dd-yyyy.
            endDate (string): date string to filter out the messages that were sent after it. Has the format of mm-dd-yyyy.
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
            fileSuffix (string): a suffix to be added to the end of the file name
        '''
        if messageId:
            messageUrl = "Message/{}".format(messageId)
            try:
                responseMessage = self.getRequest(messageUrl, log).json()
            except RuntimeError as runError:
                print("Max number of retries met. Message {} has been skipped".format(messageId))
            except KeyboardInterrupt as key:
                print("Operation Canceled")
            else:
                try:
                    data = self.isResponseEmpty(responseMessage, messageUrl, log)
                except RuntimeError as runError:
                    print("Max number of retries met. Message has been skipped.")
                    pass
                except KeyboardInterrupt as key:
                    print("Operation Canceled")
                else:
                    subject = data['subject']
                    gaName = data['googleAnalyticsCampaignName']
                    sendDate = data['sendDate']

            data = [subject, messageId, gaName, sendDate]
            data = data + self.listrakSummary(messageId, log)
            return [self.fileName]
        else:
            messages = self.allMessages(startDate, endDate, log, fileSuffix)
            for val in messages:
                messageUrl = "Message/{}".format(val)
                try:
                    responseMessage = self.getRequest(messageUrl, log).json()
                except RuntimeError as runError:
                    print("Max number of retries met. Message has been skipped.")
                    pass
                except KeyboardInterrupt as key:
                    print("Operation Canceled")
                    break
                else:
                    try:
                        data = self.isResponseEmpty(responseMessage, messageUrl, log)
                    except RuntimeError as runError:
                        print("Max number of retries met. Message has been skipped.")
                        pass
                    except KeyboardInterrupt as key:
                        print("Operation Canceled")
                        break
                    else:
                        subject = data['subject']
                        gaName = data['googleAnalyticsCampaignName']
                        sendDate = data['sendDate']
                        data = [subject, val, gaName, sendDate]
                        data = data + self.listrakSummary(val, log)
                        self.summaryOutput(data, startDate, endDate, fileSuffix)

            if log:
                if (not startDate) and (not endDate):
                    startDate = "09-01-2018"
                    endDate = datetime.datetime.now().strftime("%m-%d-%Y")
                elif not startDate:
                    startDate = "09-01-2018"
                elif not endDate:
                    endDate = datetime.datetime.now().strftime("%m-%d-%Y")
                success = ["success", datetime.datetime.now().strftime("%m/%d/%Y %H:%M"), str(self.listId),
                           "summary file created: [{},{})".format(startDate, endDate), "-"]
                self.logWriter(success)

            return [self.fileName]

    def summaryOutput(self, data, startDate, endDate, fileSuffix):
        '''
        Writes csv files for the data produced by the getSummary method

        Args:
            data (list): data that is to be written to the csv file.
            startDate (string): date string to filter out the messages that were sent before it. Has the format of mm-dd-yyyy.
            endDate (string): date string to filter out the messages that were sent after it. Has the format of mm-dd-yyyy.
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
            fileSuffix (string): a suffix to be added to the end of the file name
        '''
        fileName = str(self.listId) + "_summary_"
        if not startDate:
            fileName = fileName + "09-01-2018_"
        else:
            fileName = fileName + startDate + "_"
        if not endDate:
            fileName = fileName + datetime.datetime.now().strftime("%m-%d-%Y")
        else:
            fileName = fileName + endDate
        if fileSuffix:
            fileName = fileName + "_{}".format(fileSuffix)
        if self.summaryPath:
            fileName = self.summaryPath + fileName
        fileName = fileName + ".csv"
        self.fileName = fileName
        with open(fileName, mode = 'a+', encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            columns = [ 'brand', 'subject', 'messageId', 'gaName', 'sendDate', 'averageOrderValue',
                       'bounceCount', 'clickCount', 'conversionCount', 'openCount',
                       'passAlongCount', 'readCount', 'revenue', 'sentCount',
                       'unsubscribeCount', 'visitCount']
            if os.stat(fileName).st_size == 0:
                writer.writerow(columns)
            if self.listName:
                writer.writerow([self.listName] + data)
            else:
                writer.writerow([self.listId] + data)

    def getContacts(self, startDate = None, endDate = None, subscribed = True, segmentationFields = {}, jsonOutput = False, log = False, fileSuffix = None):
        '''
        The getContacts method handles all requests related to the Contacts endpoint. If no startDate or endDate are provided, the method will
        get the full contact list.

        Args:
            startDate (string): date string to filter out the messages that were sent before it. Has the format of mm-dd-yyyy.
            endDate (string): date string to filter out the messages that were sent after it. Has the format of mm-dd-yyyy.
            subscribed (bool): flag that filters subscribed or unsubscribed email address. When set to True, it will get
                               subscribed contact data.
            segmentationFields (dict): dictionary that holds the segmentation field id (key) and its name (value)
            log (bool): flag that indicates whether there should be a log of all activities carried out by the
                        code.
            fileSuffix (string): a suffix to be added to the end of the file name
        '''
        self.segmentationFields = segmentationFields
        if self.log:
            log = True

        contactUrl = "Contact?count=5000"
        if startDate:
            contactUrl = contactUrl + "&startDate={}".format(startDate)
        else:
            pass
        if endDate:
            contactUrl = contactUrl + "&endDate={}".format(endDate)
        else:
            pass
        if segmentationFields:
            contactUrl = contactUrl + "&segmentationFieldIds={}".format(",".join([str(key) for key, val in segmentationFields.items()]))
        else:
            pass
        if not subscribed:
            contactUrl = contactUrl + "&subscriptionState=Unsubscribed"
        else:
            pass

        nextPage = "cursor"
        counter = 0
        while nextPage:
            if counter == 0:
                nurl = contactUrl
            else:
                nurl = contactUrl + "&cursor={}".format(nextPage)
            try:
                response = self.getRequest(nurl, log).json()
            except RuntimeError as runError:
                raise runError
            except KeyboardInterrupt as key:
                raise key
            else:
                data = self.isResponseEmpty(response, nurl, log)
                if jsonOutput:
                    self.contactJsonOutput(data, counter, fileSuffix)
                else:
                    self.contactOutput(data, fileSuffix)
                nextPage = response['nextPageCursor']
                counter += 1
        if log:
            if (not startDate) and (not endDate):
                startDate = "09-01-2018"
                endDate = datetime.datetime.now().strftime("%m-%d-%Y")
            elif not startDate:
                startDate = "09-01-2018"
            elif not endDate:
                endDate = datetime.datetime.now().strftime("%m-%d-%Y")

            if subscribed:
                subs = 'Subscribed'
            else:
                subs = 'Unsubscribed'

            contactString = "contact file created: [{}, {})-{}".format(startDate, endDate, subs)
            if segmentationFields:
                contactString + "-" + ",".join([str(key) for key, val in segmentationFields.items()])

            success = ["success", datetime.datetime.now().strftime("%m/%d/%Y %H:%M"), str(self.listId),
                       contactString, "-"]

            if log:
                self.logWriter(success)

            return [self.fileName]

    def contactOutput(self, data, fileSuffix):
        """
        Writes csv files for the data produced by the getContact method.

        Args:
            data (list): list comprised of the contact information obtained from the Contact endpoint.
            fileSuffix (string): a suffix to be added to the end of the file name.
        """
        fileName = str(self.listId) + "_contacts"
        if fileSuffix:
            fileName = fileName + "_{}".format(fileSuffix)
        if self.contactPath:
            fileName = self.contactPath + fileName
        fileName = fileName + ".csv"
        self.fileName = fileName
        with open(fileName, mode = 'a+', encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            columns = ['brand','emailAddress', 'emailKey', 'subscriptionState', 'subscribeDate', 'resubscribeDate', 'subscribeMethod', 'unsubscribeDate', 'unsubscribeMethod', 'externalContactID']
            if self.segmentationFields:
                columns = columns + [value for key, value in self.segmentationFields.items()]
            if os.stat(fileName).st_size == 0:
                writer.writerow(columns)
            if self.listName:
                name = self.listName
            else:
                name = self.listId
            [writer.writerow([name] + [val for key, val in row.items() if key != 'segmentationFieldValues'] + [val['value'] for val in row['segmentationFieldValues']]) for row in data]

    def contactJsonOutput(self, data, counter, fileSuffix):
        fileName = "{}_contacts_{}".format(str(self.listId), str(counter+1))
        if fileSuffix:
            fileName = fileName + "_{}".format(fileSuffix)
        if self.contactPath:
            fileName = self.contactPath + fileName
        fileName = fileName + ".json"
        with open(fileName, 'w') as f:
            json.dump(data, f)
        return fileName

    def getConversationMessages(self, conversationId, conversationName, startDate = None, endDate = None, preVers = False, log = False, fileSuffix = None):
        if self.log:
            log = True

        conversUrl = "Conversation/{}/Message".format(conversationId)
        try:
            messageList = [conversUrl + "/{}".format(messageId['messageId']) for messageId in self.getRequest(conversUrl + "?includePreviousVersions=True" if preVers else conversUrl, log).json()['data']]
        except RuntimeError as runError:
            print("Max number of retries met. Operation Canceled")
            pass
        except KeyboardInterrupt as key:
            print("Operation Canceled")
            pass
        else:
            fileList = []
            for url in messageList:
                try:
                    fileList.extend(self.conversationMessage(url, conversationName, startDate, endDate, log, fileSuffix))
                except RuntimeError as runError:
                    print("Max number of retries met. Message has been skipped")
                    pass
                except KeyboardInterrupt as key:
                    print("Operation Canceled")
                    break
            return  fileList

    def conversationMessage(self, conversationUrl, conversationName, startDate, endDate, log, fileSuffix):
        try:
            responseMessage = self.getRequest(conversationUrl, log).json()['data']
        except RuntimeError as runError:
            raise runError
        except KeyboardInterrupt as key:
            raise key
        else:
            subject = responseMessage['subject']
            gaName = responseMessage['googleAnalyticsCampaignName']
            messageId = conversationUrl.split("/")[3]
            messageActivityUrl = conversationUrl + "/Activity?count=5000"
            if startDate:
                messageActivityUrl = messageActivityUrl + "&startDate={}".format(startDate)
            if endDate:
                messageActivityUrl = messageActivityUrl + "&endDate={}".format(endDate)

            nextPage = True
            counter = 0
            fileList = []
            while nextPage:
                if counter == 0:
                    nurl = messageActivityUrl
                else:
                    nurl = messageActivityUrl + "&cursor={}".format(nextPage)
                try:
                    response = self.getRequest(nurl, log).json()
                except RuntimeError as runError:
                    raise runError
                except KeyboardInterrupt as key:
                    raise key
                else:
                    nextPage = response['nextPageCursor']
                    response['brand'] = self.listName
                    response['gaName'] = gaName
                    response['subject'] = subject
                    response['messageId'] = messageId
                    response['conversationName'] = conversationName
                    fileList.append(self.conversationOutput(response, messageId, counter, None))
                    counter += 1
            return fileList

    def conversationOutput(self, data, messageId, counter, fileSuffix):
        fileName = str(self.listId) + "_conMessage_{}_{}".format(str(messageId), str(counter+1))
        if fileSuffix:
            fileName = fileName + "_{}".format(fileSuffix)
        if self.messagePath:
            fileName = self.messagePath + fileName
        fileName = fileName + ".json"
        with open(fileName, 'w') as f:
            json.dump(data, f)
        return fileName

    def logWriter(self, data):
        """
        The logWriter method logs the success or failure of each request made through the getRequest method

        Arg:
            timeString (string): the datetime string used to build the file name. Has the format of: mmddyyyy.
            data (list): the data that will be written in the log file.
            logPath (string): the path where the file will be saved.
        """
        activityLogName = self.logFileName
        with open(activityLogName, "a+") as f:
            f.write("|".join(data))
            f.write("\n")

if __name__== "__main__":
    client = ListrakWriter(client_id, client_secret, listId= listDict['yccUS'], contactPath = './test/')
    start = datetime.datetime.now()
    with open("json_fields/field_group_11.json", 'r') as f:
        fields = json.load(f)
    client.getContacts(startDate='08-17-2020', jsonOutput=True, segmentationFields = fields)
    end = datetime.datetime.now()
    print(end - start)
