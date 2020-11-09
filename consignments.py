import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd
from headers import APPEND_HEADERS
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
  # new_url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77'}
  new_header = {'Authorization': 'Bearer '+ token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
                'Content-Type': ''}
  body = """
  {"query":
    "query searchConsignments($ref: [String!]!, $aftercursor: String,$beforecursor: String){\
      consignments(ref: $ref, first: 70,createdOn: {from: \\"2020-10-19\\"}, after:$aftercursor, before: $beforecursor) {\
        pageInfo {hasNextPage hasPreviousPage }\
          edges { cursor node { id, ref, status, createdOn, updatedOn, trackingLabel, carrier {id,ref,status}, consignmentReference consignmentArticles(first: 10) {\
            articleEdge: edges { articleNode: node {\
              article {id, ref, status, createdOn, updatedOn, attributes {name, type, value}, fulfilments(ref: $ref) { edges {\
                node { id, order{\
                  id,ref,createdOn\
                  }\
                }\
              }\
            }\
          }\
        }\
      },}}}}}","variables":{"ref":["%"],"includeArticleItems":false,"aftercursor":\""""+cursor+"""\"}}
"""

  response = requests.post(new_url, headers=new_header, data=body)
  data = response.json()
  cursor = data['data']['consignments']['edges'][len(data['data']['consignments']['edges'])-1]['cursor']
  allData.append(data)
  if data['data']['consignments']['pageInfo']['hasNextPage'] == False:
    break

for i in allData:
  for edge in i['data']['consignments']['edges']:
    newData.append(edge)

allNewData = []
data_length = len(newData)-1

while data_length != -1:
  single_row = newData[data_length]
  for edge_row in single_row['node']['consignmentArticles']['articleEdge']:
    allNewData.append([
        '{}{}'.format(edge_row['articleNode']['article']['fulfilments']['edges'][0]['node']['order']['ref'],newData[data_length]['node']['ref']),
        newData[data_length]['node']['id'],
        newData[data_length]['node']['ref'],
        newData[data_length]['node']['status'],
        newData[data_length]['node']['createdOn'],
        newData[data_length]['node']['updatedOn'],
        newData[data_length]['node']['carrier']['id'],
        newData[data_length]['node']['carrier']['ref'],
        newData[data_length]['node']['trackingLabel'],
        newData[data_length]['node']['consignmentReference'],
        edge_row['articleNode']['article']['fulfilments']['edges'][0]['node']['order']['ref'],
        edge_row['articleNode']['article']['id'],
        edge_row['articleNode']['article']['ref'],
        edge_row['articleNode']['article']['createdOn'],
        edge_row['articleNode']['article']['updatedOn'],
        edge_row['articleNode']['article']['status'],
        json.dumps(edge_row['articleNode']['article']['attributes']),
        json.dumps(edge_row['articleNode']['article']['fulfilments']['edges']),
        next((shipment['value'] for shipment in edge_row['articleNode']['article']['attributes'] if shipment['name'] == 'shipmentDatetime'), None) if edge_row['articleNode']['article']['attributes']  else '',
        edge_row['articleNode']['article']['fulfilments']['edges'][0]['node']['id']
    ])
  data_length = data_length - 1

original_row = [
    'primary_key',
    'node_id',
    'node_ref',
    'node_status',
    'node_createdOn',
    'node_updatedOn',
    'node_carrier_id',
    'node_carrier_ref',
    'node_tracking_label',
    'node_consignmentReference',
    'fulfilment_ref',
    'node_article_id',
    'node_article_ref',
    'node_article_createdOn',
    'node_article_updatedOn',
    'node_article_status',
    'node_article_attributes',
    'node_article_fulfilments_edges',
    'shipment_date',
    'node_consignmentArticles_articleEdge_articleNode_article_fulfilments_edges_node_id'
]

city = pd.DataFrame(allNewData, columns=original_row)
city.to_csv('consignments.csv')
