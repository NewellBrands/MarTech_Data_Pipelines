# Listrak Pipelines

This is the code repository for Listrak API related libraries and pipelines.

## ListrakWriter Library

The *listrakwriter.py* library was designed to pull data for triggered and promotional emails and contacts through the Listrak API. It's currently being used in the Listrak Pipelines for the **List Health, Deliverability and Conversation Reports**. It relies on a support script called *utils* which has API credentials and unique brand identifiers used in the platform, among other data.

### Usage

```python
# Importing dependencies
from listrakwriter import ListrakWriter
import utils

# Defining Listrak's unique identifier for Marmot
marmot = utils.listDict['marmot']

# Initiating the ListrakWriter object for Marmot
client = ListrakWriter(utils.client_id, utils.client_secret, marmot, listName = 'Marmot')

# Pulling all contact data for subscribers
client.getContacts()

# Pulling data for messages sent out during the first week of 2020
client.getMessages(startDate = '01-01-2020', endDate = '01-07-2020')
```

## LTK Pipeline Deliverability Report Pipeline

The **LTK Deliverability Report Pipeline** runs from the *MarTech Virtual Machine* (MVM) every Tuesday at 12 AM; the script that runs this is called *pipeline_deliverability_conversation.py*. This script relies on the following three libraries:

* ListrakWriter Library: it interacts with Listrak’s API, pulls the requested data and writes as CSV files to the desired folder in the MVM.

* Utils script: it provides all the required credentials for both the **ListrakWriter and Transfer** libraries.

* Transfer Library: it queues up each of the data pulls for each of the brands, sends the files over to the **Azure Datalake** and triggers the **LTK_Deliverability_Report Job** in Databricks once each of these is over.
