import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd
from headers import APPEND_HEADERS
from decouple import config 

def get_unique_numbers(numbers):
    unique = []
    for number in numbers:
        if number not in unique:
            unique.append(number)
    return unique


def flatten_jsony(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


def flatten_json(y):
  out = {}

  def flatten(x, name=''):
      if type(x) is dict:
          for a in x:
              flatten(x[a], name + a + '_')
      elif type(x) is list:
          i = 0
          for a in x:
              flatten(a, name + str(i) + '_')
      else:
          out[name[:-1]] = x
  flatten(y)
  return out
allReturns = []
users = 3
retailerId = "1"
token = ''
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
  allData = []
  newData = []
  cursor = ""
  while hasNextP:
    new_url = "https://sssports.api.fluentretail.com/graphql"
    # new_url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77'}
    new_header = {'Authorization': 'Bearer '+ token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
                  'Content-Type': ''}
    body = """{"query":\
      "query getReturnFulfilment ($ref: [String!]) {\
        returnFulfilments(ref:$ref,createdOn: {from: \\"2020-10-19\\"},first: 100) {\
          pageInfo { hasNextPage  hasPreviousPage } edges {\
            cursor node  { id, ref, type, returnOrder {  ref,  id,  order { ref }  id,  ref }\
              status,createdOn,updatedOn,attributes {  name  type  value } returnFulfilmentItems {\
                returnFulfilmentItemEdges: edges { returnFulfilmentItemNode: node { id, ref, createdOn, product { ref }, unitQuantity { quantity }\
                  returnFulfilmentItemAttriutes: attributes {name,type,value  }\
                        }\
                      }\
                    }\
                  }\
                }\
              }\
            }",\
          "variables":{"retailerId":\""""+retailerId+"""\", "cursor": \""""+cursor+"""\"}\
      }
    """

    response = requests.post(new_url, headers=new_header, data=body)
    data = response.json()
    if len(data['data']['returnFulfilments']['edges']) == 0:
      break
    else:
      cursor = data['data']['returnFulfilments']['edges'][len(data['data']['returnFulfilments']['edges'])-1]['cursor']
      allData.append(data)
    for i in allData:
      for edge in i['data']['returnFulfilments']['edges']:
        newData.append(edge)
    header_data = flatten_json(newData)
    headers_data = flatten_jsony(newData)
    row = []
    original_row = []
    for key in header_data.keys():
      original_row.append(key.replace('data_', "").replace('0_', ""))

    for key in headers_data.keys():
      row.append(key.replace('data_', ""))

    allNewData = []
    data_length = len(newData)-1
    while data_length != -1:
      single_row = newData[data_length]
      for edge_row in single_row['node']['returnFulfilmentItems']['returnFulfilmentItemEdges']:
        allNewData.append([
            newData[data_length]['cursor'],
            newData[data_length]['node']['id'],
            newData[data_length]['node']['ref'],
            newData[data_length]['node']['type'],
            newData[data_length]['node']['status'],
            newData[data_length]['node']['createdOn'],
            newData[data_length]['node']['updatedOn'],
            newData[data_length]['node']['returnOrder']['id'],
            newData[data_length]['node']['returnOrder']['ref'],
            newData[data_length]['node']['returnOrder']['order']['ref'],
            newData[data_length]['node']['attributes'],
            edge_row['returnFulfilmentItemNode']['id'],
            edge_row['returnFulfilmentItemNode']['ref'],
            edge_row['returnFulfilmentItemNode']['createdOn'],
            edge_row['returnFulfilmentItemNode']['product']['ref'],
            edge_row['returnFulfilmentItemNode']['unitQuantity']['quantity'],
            edge_row['returnFulfilmentItemNode']['returnFulfilmentItemAttriutes'],
        ])
      data_length = data_length - 1
    allReturns.append(allNewData)
    if data['data']['returnFulfilments']['pageInfo']['hasNextPage'] == False:
      break
  users = users - 1
original_row = [
    'cursor',
    'node_id',
    'node_ref',
    'node_type',
    'node_status',
    'node_createdOn',
    'node_updatedOn',
    'node_return_order_id',
    'node_return_order_ref',
    'node_return_order_orders_ref',
    'node_attributes',
    'node_returnFulfilmentItemNode_id',
    'node_returnFulfilmentItemNode_ref',
    'node_returnFulfilmentItemNode_createdOn',
    'node_returnFulfilmentItemNode_product_ref',
    'node_returnFulfilmentItemNode_unit_quantity',
    'node_returnFulfilmentItemNode_attributes'
]

csvReturns = []
for ret in allReturns:
  csvReturns = csvReturns + ret
city = pd.DataFrame(csvReturns, columns=original_row)
city.to_csv('return_fulfilments.csv')
