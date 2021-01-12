import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd
from ats_headers import ATS_HEADERS

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
new_url = "https://production-eu01-sunandsand.demandware.net/s/-/dw/data/v20_2/product_search?site_id=Kuwait"
new_url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77'}
new_header = {'Authorization': 'Bearer ' + token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
              'Content-Type': 'application/json;charset=UTF-8'}

total = 1
totalFlag = 1
allNewData = []
start = 0
count = 200
recevied = 1

while total != 0:
  body = """
      {"query" : {
      "text_query": {
      "fields": ["catalog_id"],
      "search_phrase": "akeneo-nav-catalog"
      }
      },
    "expand" : ["availability"],
    "start": """+str(start)+""",
    "count":"""+str(count)+""",
    "select" : "(hits.(id, name.(en), price, price_currency, brand, ats, in_stock, creation_date, last_modified, online, primary_category_id, c_SAPProductName, c_ah2text, c_articleNo, c_costPriceAED, c_fluentImage, c_isBigBox, c_isReturnable),total)",
    "sorts" : [{"field":"id", "sort_order":"asc"}]
    }"""
  response = requests.post(new_url, headers=new_header, data=body)
  data = response.json()
  if totalFlag == 1:
    total = math.ceil(data['total']/200)
    totalFlag  = 0
  if len(data['hits']):
    data_length = len(data['hits']) - 1
    while data_length != -1:
      allNewData.append([
        '{}{}'.format(data['hits'][data_length]['id'],data['hits'][data_length]['creation_date']),
        json.dumps(data['hits'][data_length]['id']),
        data['hits'][data_length]['name']['en'] if 'name' in data['hits'][data_length] and 'en' in data['hits'][data_length]['name']  else '',
        data['hits'][data_length]['brand'] if 'brand' in data['hits'][data_length] else '',
        data['hits'][data_length]['ats'] if 'ats' in data['hits'][data_length] else '',
        data['hits'][data_length]['in_stock'] if 'in_stock' in data['hits'][data_length] else '',
        data['hits'][data_length]['creation_date'] if 'creation_date' in data['hits'][data_length] else '',
        data['hits'][data_length]['last_modified'] if 'last_modified' in data['hits'][data_length] else '',
        data['hits'][data_length]['online'] if 'online' in data['hits'][data_length] else '',
        data['hits'][data_length]['primary_category_id'] if 'primary_category_id' in data['hits'][data_length] else '',
        data['hits'][data_length]['c_SAPProductName'] if 'c_SAPProductName' in data['hits'][data_length] else '',
        data['hits'][data_length]['c_ah2text'] if 'c_articleNo' in data['hits'][data_length] else '',
        data['hits'][data_length]['c_articleNo'] if 'c_articleNo' in data['hits'][data_length] else '',
        data['hits'][data_length]['c_costPriceAED'] if 'c_costPriceAED' in data['hits'][data_length] else '',
        data['hits'][data_length]['c_fluentImage'] if 'c_fluentImage' in data['hits'][data_length] else '',
        data['hits'][data_length]['c_isBigBox'] if 'c_isBigBox' in data['hits'][data_length] else '',
        data['hits'][data_length]['c_isReturnable'] if 'c_isReturnable' in data['hits'][data_length] else '',
        'KUWAIT'
      ])
      data_length = data_length - 1
    start = recevied * 200
    recevied = recevied + 1
    total = total - 1

city = pd.DataFrame(allNewData, columns=ATS_HEADERS)
city.to_csv('products_kuwait.csv')
