import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd
from headers import APPEND_HEADERS

url = "https://account.demandware.com/dw/oauth2/access_token?client_id={{client_id}}"
url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77','grant_type': 'client_credentials'}
authen = HTTPBasicAuth('ce6abb4e-faf1-41af-94e7-feb1e2dd4a77', 'ae9l8yKmKT5rNjy')
header = {'Content-Type': 'application/x-www-form-urlencoded'}
response = requests.post(url, auth=authen, params=url_param, headers=header)
response_dict = json.loads(response.text)
token = response_dict["access_token"]
result = []
new_url = "https://production-eu01-sunandsand.demandware.net/s/KSA/dw/shop/v20_2/order_search"
new_url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77'}
new_header = {'Authorization': 'Bearer ' + token, 'Origin': 'https://production-eu01-sunandsand.demandware.net','Content-Type': 'application/json;charset=UTF-8'}

total = 1
totalFlag = 1
allNewData = []
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
                    "from": "2020-10-19"
                }
            },
            "query" : {
                "match_all_query": {}
            }
        }
    },
    "start": """+str(start)+""",
    "count":"""+str(count)+""",
    "select" : "(**)",
    "sorts" : [{"field":"creation_date", "sort_order":"asc"}]
    }"""
  response = requests.post(new_url, headers=new_header, data=body)
  data = response.json()
  if totalFlag == 1:
    total = math.ceil(data['total']/200)
    totalFlag = 0
  if total % 100 == 0:
    url = "https://account.demandware.com/dw/oauth2/access_token?client_id={{client_id}}"
    url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77','grant_type': 'client_credentials'}
    authen = HTTPBasicAuth('ce6abb4e-faf1-41af-94e7-feb1e2dd4a77', 'ae9l8yKmKT5rNjy')
    header = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(url, auth=authen, params=url_param, headers=header)
    response_dict = json.loads(response.text)
    token = response_dict["access_token"]
    result = []
    new_url = "https://production-eu01-sunandsand.demandware.net/s/KSA/dw/shop/v20_2/order_search"
    new_url_param = {'client_id': 'ce6abb4e-faf1-41af-94e7-feb1e2dd4a77'}
    new_header = {'Authorization': 'Bearer ' + token, 'Origin': 'https://production-eu01-sunandsand.demandware.net','Content-Type': 'application/json;charset=UTF-8'}
  if 'hits' in data and len(data['hits']):
    data_length = len(data['hits']) - 1
    while data_length != -1:
      single_row = data['hits'][data_length]
      for edge_row in single_row['data']['product_items']:
        allNewData.append([
            "{}{}".format(data['hits'][data_length]['data']['order_no'], edge_row['product_id']),
            data['hits'][data_length]['data']['_type'],
            data['hits'][data_length]['data']['adjusted_merchandize_total_tax'],
            data['hits'][data_length]['data']['adjusted_shipping_total_tax'],
            data['hits'][data_length]['data']['confirmation_status'],
            data['hits'][data_length]['data']['created_by'],
            data['hits'][data_length]['data']['creation_date'],
            data['hits'][data_length]['data']['currency'],
            data['hits'][data_length]['data']['customer_info']['_type'] if '_type' in data['hits'][data_length]['data']['customer_info'] else '',
            data['hits'][data_length]['data']['customer_info']['customer_id'] if 'customer_id' in data['hits'][data_length]['data']['customer_info'] else '',
            data['hits'][data_length]['data']['customer_info']['customer_name'] if 'customer_name' in data['hits'][data_length]['data']['customer_info'] else '',
            data['hits'][data_length]['data']['customer_info']['customer_no'] if 'customer_no' in data['hits'][data_length]['data']['customer_info'] else '',
            data['hits'][data_length]['data']['customer_info']['email'] if 'email' in data['hits'][data_length]['data']['customer_info'] else '',
            data['hits'][data_length]['data']['c_customerPhoneNumber'] if 'c_customerPhoneNumber' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['billing_address']['_type'] if '_type' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['address1'] if 'address1' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['city'] if 'city' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['country_code'] if 'country_code' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['first_name'] if 'first_name' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['full_name'] if 'full_name' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['id'] if 'id' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['last_name'] if 'last_name' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['phone'] if 'phone' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['salutation'] if 'salutation' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['c_area'] if 'c_area' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['c_email'] if 'c_email' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['billing_address']['c_phoneWhatsApp'] if 'c_phoneWhatsApp' in data['hits'][data_length]['data']['billing_address'] else '',
            data['hits'][data_length]['data']['export_status'],
            data['hits'][data_length]['data']['last_modified'],
            data['hits'][data_length]['data']['merchandize_total_tax'],
            data['hits'][data_length]['data']['notes']['_type'],
            data['hits'][data_length]['data']['notes']['link'],
            data['hits'][data_length]['data']['order_no'],
            json.dumps(data['hits'][data_length]['data']['order_price_adjustments']) if 'order_price_adjustments' in data['hits'][data_length]['data'] else '',
            json.dumps([item['price_adjustments'] for item in data['hits'][data_length]['data']['product_items'] if 'price_adjustments' in item][0]) if len([item['price_adjustments'] for item in data['hits'][data_length]['data']['product_items'] if 'price_adjustments' in item]) else '',
            data['hits'][data_length]['data']['order_token'],
            data['hits'][data_length]['data']['order_total'],
            json.dumps(data['hits'][data_length]['data']['payment_instruments']),
            data['hits'][data_length]['data']['payment_status'],
            edge_row['_type'],
            edge_row['adjusted_tax'],
            edge_row['base_price'],
            edge_row['bonus_product_line_item'],
            edge_row['gift'],
            edge_row['item_id'],
            edge_row['item_text'],
            edge_row['price'],
            edge_row['price_after_item_discount'],
            edge_row['price_after_order_discount'],
            edge_row['product_id'],
            edge_row['product_name'],
            edge_row['quantity'],
            edge_row['shipment_id'],
            edge_row['tax'],
            edge_row['tax_basis'],
            edge_row['tax_class_id'],
            edge_row['tax_rate'],
            edge_row['c_isBigBox'],
            edge_row['c_isCCAvailable'],
            edge_row['c_listPrice'],
            edge_row['c_maxBuyableQuantity'],
            edge_row['c_productImage'],
            edge_row['c_proratedPrice'] if 'c_proratedPrice' in edge_row else '',
            edge_row['c_selectedShoeFormat'] if 'c_selectedShoeFormat' in edge_row else '',
            edge_row['c_selectedSize'] if 'c_selectedSize' in edge_row else '',
            data['hits'][data_length]['data']['product_sub_total'],
            data['hits'][data_length]['data']['product_total'],
            json.dumps(data['hits'][data_length]['data']['shipments']),
            data['hits'][data_length]['data']['shipments'][0]['shipping_method']['_type'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_method']['id'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_method']['name'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_method']['c_estimatedArrivalTime'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_method']['c_fluentShippingCode'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_method']['c_hideForBigBox'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_method']['c_storePickupEnabled'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_status'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_total'],
            data['hits'][data_length]['data']['shipments'][0]['shipping_total_tax'],
            data['hits'][data_length]['data']['shipments'][0]['tax_total'],
            data['hits'][data_length]['data']['shipping_items'],
            data['hits'][data_length]['data']['shipping_status'],
            data['hits'][data_length]['data']['shipping_total'],
            data['hits'][data_length]['data']['shipping_total_tax'],
            data['hits'][data_length]['data']['site_id'] if 'site_id' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['status'] if 'status' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['taxation'] if 'taxation' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['tax_total'] if 'tax_total' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_carrier'] if 'c_carrier' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_customOrderStatus'] if 'c_customOrderStatus' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_fluentOrderId'] if 'c_fluentOrderId' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_fluentOrderUpdateJsonData'] if 'c_fluentOrderUpdateJsonData' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_latitude'] if 'c_latitude' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_longitude'] if 'c_longitude' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_orderPushedToFluent'] if 'c_orderPushedToFluent' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_orderPushedToFluentDate'] if 'c_orderPushedToFluentDate' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_orderPushedToFluentError'] if 'c_orderPushedToFluentError' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_placedLocale'] if 'c_placedLocale' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_returnOrderIds'] if 'c_returnOrderIds' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_sscContactId'] if 'c_sscContactId' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_sscSyncResponseText'] if 'c_sscSyncResponseText' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_sscSyncStatus'] if 'c_sscSyncStatus' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_channel'] if 'c_channel' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_sscid'] if 'c_sscid' in data['hits'][data_length]['data'] else '',
            json.dumps(json.loads(data['hits'][data_length]['data']['c_refundHistory'])) if 'c_refundHistory' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_lastPaymentStatus'] if 'c_lastPaymentStatus' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_paymentType'] if 'c_paymentType' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['relevance'] if 'relevance' in data['hits'][data_length] else '',
            data['hits'][data_length]['data']['c_cancellationReason'] if 'c_cancellationReason' in data['hits'][data_length]['data'] else '',
            data['hits'][data_length]['data']['c_cancellationReasonText'] if 'c_cancellationReasonText' in data['hits'][data_length]['data'] else '',
        ])
      data_length = data_length - 1
    start = recevied * 200
    recevied = recevied + 1
    total = total - 1

city = pd.DataFrame(allNewData, columns=APPEND_HEADERS)
city.to_csv('orders_ksa.csv')
