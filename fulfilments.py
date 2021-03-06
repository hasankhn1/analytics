import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd
from decouple import config 

url = "https://sssports.api.fluentretail.com/oauth/token?username={}&password={}&client_id=SSSPORTS&client_secret=ce3304b2-6e2a-4922-bf6c-24515b623361&grant_type=password&scope=api".format(config('UAE_USERNAME'),config('UAE_PASSWORD'))
response = requests.request("POST", url, headers={}, data = {})
response_dict = json.loads(response.text)
token = response_dict["access_token"]

hasNextP = True
allData = []
newData = []
cursor = ""
while hasNextP:
  new_url = "https://sssports.api.fluentretail.com/graphql"
  new_header = {'Authorization': 'Bearer '+ token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
                'Content-Type': ''}
  body = """
        {"query":"query Fulfilments($cursor:String){\
        fulfilments(createdOn: {from: \\"2020-10-19\\"},first: 50, after:$cursor) {\
          pageInfo { hasNextPage, hasPreviousPage } edges {\
          cursor, node { id, ref, order{ id, ref, createdOn, updatedOn, status } status, type, createdOn, updatedOn, toAddress {\
          id, ref }, fromAddress { id, ref }, attributes { type, value, name }, items(first: 50) {\
          itemEdge: edges { itemNode: node {\
          id, ref, status, requestedQuantity, filledQuantity, rejectedQuantity, orderItem { id, ref, status, quantity, currency, paidPrice product { ... on VariantProduct { ref } } }\
          } } } } } }}",\
          "variables":{"ref":["%"],"cursor":\""""+cursor+"""\"}}
"""
  response = requests.post(new_url, headers=new_header, data=body)
  data = response.json()
  cursor = data['data']['fulfilments']['edges'][len(data['data']['fulfilments']['edges'])-1]['cursor']
  allData.append(data)
  if data['data']['fulfilments']['pageInfo']['hasNextPage'] == False:
    break
for i in allData:
  for edge in i['data']['fulfilments']['edges']:
    newData.append(edge)

allNewData = []
data_length = len(newData)-1
while data_length != -1:
  single_row = newData[data_length]
  for edge_row in single_row['node']['items']['itemEdge']:
    allNewData.append([
        '{}{}'.format(edge_row['itemNode']['id'],newData[data_length]['node']['order']['ref']),
        newData[data_length]['cursor'],
        newData[data_length]['node']['id'],
        newData[data_length]['node']['ref'],
        newData[data_length]['node']['order']['id'],
        newData[data_length]['node']['order']['ref'],
        newData[data_length]['node']['order']['createdOn'],
        newData[data_length]['node']['order']['updatedOn'],
        newData[data_length]['node']['status'],
        newData[data_length]['node']['type'],
        newData[data_length]['node']['createdOn'],
        newData[data_length]['node']['updatedOn'],
        newData[data_length]['node']['toAddress'],
        newData[data_length]['node']['fromAddress'],
        newData[data_length]['node']['attributes'][0]['type'],
        newData[data_length]['node']['attributes'][0]['name'],
        newData[data_length]['node']['attributes'][0]['value'],
        newData[data_length]['node']['attributes'][1]['type'] if len(newData[data_length]['node']['attributes']) > 1 else '',
        newData[data_length]['node']['attributes'][1]['name'] if len(newData[data_length]['node']['attributes']) > 1 else  '',
        newData[data_length]['node']['attributes'][1]['value'] if len(newData[data_length]['node']['attributes']) >1 else '',
        edge_row['itemNode']['id'],
        edge_row['itemNode']['ref'],
        edge_row['itemNode']['status'],
        edge_row['itemNode']['requestedQuantity'],
        edge_row['itemNode']['filledQuantity'],
        edge_row['itemNode']['rejectedQuantity'],
        edge_row['itemNode']['orderItem']['id'],
        edge_row['itemNode']['orderItem']['ref'],
        edge_row['itemNode']['orderItem']['status'],
        edge_row['itemNode']['orderItem']['quantity'],
        edge_row['itemNode']['orderItem']['currency'],
        edge_row['itemNode']['orderItem']['paidPrice'],
        json.dumps(edge_row['itemNode']['orderItem']['product']['ref'])
    ])
  data_length = data_length - 1
original_row = [
    'primary_key',
    'cursor',
    'node_id',
    'node_ref',
    'node_order_id',
    'node_order_ref',
    'node_order_createdOn',
    'node_order_updatedOn',
    'node_status',
    'node_type',
    'node_createdOn',
    'node_updatedOn',
    'node_toAddress',
    'node_fromAddress',
    'node_attributes_name_0',
    'node_attributes_value_0',
    'node_attributes_type_0',
    'node_attributes_type',
    'node_attributes_value',
    'node_attributes_name',
    'node_items_itemEdge_itemNode_id',
    'node_items_itemEdge_itemNode_ref',
    'node_items_itemEdge_itemNode_status',
    'node_items_itemEdge_itemNode_requestedQuantity',
    'node_items_itemEdge_itemNode_filledQuantity',
    'node_items_itemEdge_itemNode_rejectedQuantity',
    'node_items_itemEdge_itemNode_orderItem_id',
    'node_items_itemEdge_itemNode_orderItem_ref',
    'node_items_itemEdge_itemNode_orderItem_status',
    'node_items_itemEdge_itemNode_orderItem_quantity',
    'node_items_itemEdge_itemNode_orderItem_currency',
    'node_items_itemEdge_itemNode_orderItem_paid_price',
    'node_items_itemEdge_itemNode_orderItem_product_ref'
]
city = pd.DataFrame(allNewData, columns=original_row)
city.to_csv('fulfilments.csv')
