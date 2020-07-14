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
