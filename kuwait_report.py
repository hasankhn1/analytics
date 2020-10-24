import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd


def get_unique_numbers(numbers):
    unique = []
    for number in numbers:
        if number not in unique:
            unique.append(number)
    return unique

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
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out


url = "https://account.demandware.com/dw/oauth2/access_token?client_id={{client_id}}"
url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77',
             'grant_type': 'client_credentials'}
authen = HTTPBasicAuth(
    'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77', 'ae9l8yKmKT5rNjy')
header = {'Content-Type': 'application/x-www-form-urlencoded'}
response = requests.post(url, auth=authen, params=url_param, headers=header)
response_dict = json.loads(response.text)
token = response_dict["access_token"]
result = []
new_url = "https://production-eu01-sunandsand.demandware.net/s/Kuwait/dw/shop/v20_2/order_search"
new_url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77'}
new_header = {'Authorization': 'Bearer ' + token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
              'Content-Type': 'application/json;charset=UTF-8'}
body = """
    {"query" : {
        "filtered_query": {
            "filter": {
                "range_filter": {
                    "field": "creation_date",
                    "from": "2020-10-18T00:00:00.000Z"
                }
            },
            "query" : {
                "match_all_query": {}
            }
        }
    },
      "start": """+str(0)+""",
     "count":"""+str(1)+""",
    "select" : "(**)",
    "sorts" : [{"field":"creation_date", "sort_order":"asc"}]
}"""
response = requests.post(new_url, headers=new_header, data=body)
data = response.json()
header_data = flatten_json(data['hits'][0])
row = []
original_row = []
for key in header_data.keys():
  if key == 'data__type':
    original_row.append(key)
    row.append(key)
  else:
    row.append(key.replace('data_', ""))
    original_row.append(key.replace('data_', "").replace('_0', ""))
new_indexes = []
count = 0
for i in range(len(original_row)):
  
  for num in range(30):
    if 'items_'+str(num) in original_row[i]:
      new_indexes.append(count)
  count = count + 1

for o in new_indexes:
  del original_row[76]

ALL_ROWS=[]

total = math.ceil(data['total']/200)
start = 0
count = 200
recevied = 1

while total != 0:
  body = """
    {"query" : {
        "filtered_query": {
            "filter": {
                "range_filter": {
                    "field": "creation_date",
                    "from": "2020-10-18T00:00:00.000Z"
                }
            },
            "query" : {
                "match_all_query": {}
            }
        }
    },
    "start": """+str(start)+""",
    "count":"""+str(200)+""",
    "select" : "(**)",
    "sorts" : [{"field":"creation_date", "sort_order":"asc"}]
    }"""
  response = requests.post(new_url, headers=new_header, data=body)
  data = response.json()
  if len(data['hits']):
    data_length = len(data['hits']) - 1
    while data_length != -1:
      result.append(data['hits'][data_length]['data'])
      new_flat_json_data = flatten_json(data['hits'][data_length]['data'])
      row_data = []
      product_items_length = len(
          data['hits'][data_length]['data']['product_items'])
      if product_items_length > 1:
        for product_item in data['hits'][data_length]['data']['product_items']:
          new_row_data = []
          for x in row:
            if x == 'data__type':
              new_row_data.append('order')
            elif product_item.get(x.replace('product_items_0_', '')):
              new_row_data.append(
                  product_item[x.replace('product_items_0_', '')])
            elif new_flat_json_data.get(x):
              new_row_data.append(new_flat_json_data[x])
            else:
              new_row_data.append('')
          for dl in new_indexes:
            del new_row_data[76]
          ALL_ROWS.append(new_row_data)
      else:
        for x in row:
          if x == 'data__type':
            row_data.append('order')
          elif new_flat_json_data.get(x):
            row_data.append(new_flat_json_data[x])
          else:
            row_data.append('')
        for dl in new_indexes:
          del row_data[76]
        ALL_ROWS.append(row_data)
      product_items_length = product_items_length - 1
      data_length = data_length - 1
  start = recevied * 200
  count = (recevied + 1) * 200
  recevied = recevied + 1
  total = total-1

city = pd.DataFrame(ALL_ROWS, columns=original_row)
city.to_csv('orders_kuwait.csv')
