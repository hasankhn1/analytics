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

url = "https://sssports.api.fluentretail.com/oauth/token?username={}&password={}&client_id=SSSPORTS&client_secret=ce3304b2-6e2a-4922-bf6c-24515b623361&grant_type=password&scope=api".format(config('UAE_USERNAME'),config('UAE_PASSWORD'))
response = requests.request("POST", url, headers={}, data = {})
response_dict = json.loads(response.text)
token = response_dict["access_token"]

headers = {}
original_row = []
hasNextP = True
allData = []
newData = []
cursor = ""
while hasNextP:
  new_url = "https://sssports.api.fluentretail.com/graphql"
  new_header = {'Authorization': 'Bearer '+ token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
                'Content-Type': ''}
  body = """
    {"query":
      "query searchConsignments($ref: [String!]!, $aftercursor: String,$beforecursor: String){\
        consignments(ref: $ref, first: 70,createdOn: {from: \\"2020-10-19\\"}, after:$aftercursor, before: $beforecursor) {\
          pageInfo {hasNextPage hasPreviousPage }\
            edges { cursor node { id, ref, status, createdOn, updatedOn, trackingLabel, carrier {id,ref,status}, consignmentReference consignmentArticles(first: 10) {\
              articleEdge: edges { articleNode: node {\
                article {id, ref, status, fulfilments(ref: $ref) { edges {\
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
header_data = flatten_json(newData)
headers_data = flatten_jsony(newData)
row = []
for key in header_data.keys():
  original_row.append(key.replace('data_', "").replace('0_', ""))
for key in headers_data.keys():
  row.append(key.replace('data_', ""))

row_data = []
new_flat_json_data = flatten_jsony(newData)
count = 0
allNewData = []
for x in row:
  count = count + 1
  if new_flat_json_data.get(x):
    row_data.append(new_flat_json_data[x])
  else:
    row_data.append('')
  if count == 18:
    count = 0
    allNewData.append(row_data)
    row_data = []

city = pd.DataFrame(allNewData, columns=original_row)
city.to_csv('consignments.csv')