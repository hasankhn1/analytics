import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd
from decouple import config 


allReturns = []
users = 3
retailerId = "1"
token = ''
allData = []

while users != 0:
  if users == 1:
    url = "https://sssports.api.fluentretail.com/oauth/token?username={}&password={}&client_id=SSSPORTS&client_secret=ce3304b2-6e2a-4922-bf6c-24515b623361&grant_type=password&scope=api".format(config('UAE_USERNAME'),config('UAE_PASSWORD'))
    response = requests.request("POST", url, headers={}, data = {})
    response_dict = json.loads(response.text)
    token = response_dict["access_token"]
    retailerId = "1"
  if users == 2:
    url = "https://sssports.api.fluentretail.com/oauth/token?username={}&password={}&client_id=SSSPORTS&client_secret=ce3304b2-6e2a-4922-bf6c-24515b623361&grant_type=password&scope=api".format(config('KSA_USERNAME'),config('KSA_PASSWORD'))
    response = requests.request("POST", url, headers={}, data = {})
    response_dict = json.loads(response.text)
    token = response_dict["access_token"]
    retailerId = "2"
  if users == 3:
    url = "https://sssports.api.fluentretail.com/oauth/token?username={}&password={}&client_id=SSSPORTS&client_secret=ce3304b2-6e2a-4922-bf6c-24515b623361&grant_type=password&scope=api".format(config('KUWAIT_USERNAME'),config('KUWAIT_PASSWORD'))
    response = requests.request("POST", url, headers={}, data = {})
    response_dict = json.loads(response.text)
    token = response_dict["access_token"]
    retailerId = "3"
  hasNextP = True
  newData = []
  cursor = ""
  while hasNextP:
    new_url = "https://sssports.api.fluentretail.com/graphql"
    # new_url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77'}
    new_header = {'Authorization': 'Bearer '+ token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
                  'Content-Type': ''}
    body = """{"query":\
      "query searchReturnOrder ($retailerId: ID!, $ref : [String!], $cursor:String, $exchangeOrder: OrderLinkInput,  $includeAttributes: Boolean!) {\
        returnOrders (first: 100, ref: $ref,  createdOn: {from:\\"2020-10-19\\"}, retailer: { id: $retailerId }, exchangeOrder: $exchangeOrder, after: $cursor) {\
          pageInfo{hasNextPage}\
          edges { cursor node {ref id type status createdOn updatedOn currency { alphabeticCode }exchangeOrder {\
            ref }order { ref }retailer { id }subTotalAmount { amount } totalAmount { amount }customer { ref } exchangeOrder { ref }\
              lodgedLocation{ ref } destinationLocation { ref } retailer { id } attributes @include(if: $includeAttributes) {\
                name type value }\
                }\
              }\
            }\
          }",\
        "variables":{"retailerId":\""""+retailerId+"""\","includeAttributes":true,"includeFulfilment":true, "cursor": \""""+cursor+"""\"\
      }\
    }"""

    response = requests.post(new_url, headers=new_header, data=body)
    data = response.json()
    for edge in data['data']['returnOrders']['edges']:
      allData.append(edge)
    if not data['data']['returnOrders']['edges'] and data['data']['returnOrders']['pageInfo']['hasNextPage'] == False:
      users = users - 1
      break
    else:
      cursor = data['data']['returnOrders']['edges'][len(data['data']['returnOrders']['edges'])-1]['cursor']

allNewData = []
data_length = len(allData)-1
print(json.dumps(allData))
while data_length != -1:
  single_row = allData[data_length]
  allNewData.append([
      allData[data_length]['cursor'],
      allData[data_length]['node']['id'],
      allData[data_length]['node']['ref'],
      allData[data_length]['node']['type'],
      allData[data_length]['node']['status'],
      allData[data_length]['node']['createdOn'],
      allData[data_length]['node']['updatedOn'],
      allData[data_length]['node']['currency']['alphabeticCode'],
      allData[data_length]['node']['exchangeOrder'],
      allData[data_length]['node']['order']['ref'],
      allData[data_length]['node']['retailer']['id'],
      allData[data_length]['node']['subTotalAmount']['amount'],
      allData[data_length]['node']['totalAmount']['amount'],
      allData[data_length]['node']['customer']['ref'],
      allData[data_length]['node']['lodgedLocation'],
      allData[data_length]['node']['destinationLocation'],
      json.dumps(allData[data_length]['node']['attributes']),
  ])
  data_length = data_length - 1

original_row = [
    'cursor',
    'node_id',
    'node_ref',
    'node_type',
    'node_status',
    'node_createdOn',
    'node_updatedOn',
    'node_currency_alphabeticCode',
    'node_currency_exhangeOrder',
    'node_order_ref',
    'node_retailer_id',
    'node_subtotal_amout',
    'node_total_amount',
    'node_customer_ref',
    'node_lodge_location',
    'node_destination_location',
    'node_attributes'
]

city = pd.DataFrame(allNewData, columns=original_row)
city.to_csv('return_order.csv')
