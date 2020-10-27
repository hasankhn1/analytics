from flask_restful import Resource, Api, request
import requests
import json
from requests.auth import HTTPBasicAuth
import math
import csv
import pandas as pd
from headers import APPEND_HEADERS
import numpy as np
from decouple import config

#ApI FOR TESTING RESOURCES
class Item(Resource):
  def get(self):
    url = "https://sssports.api.fluentretail.com/oauth/token?username={}&password={}&client_id=SSSPORTS&client_secret=ce3304b2-6e2a-4922-bf6c-24515b623361&grant_type=password&scope=api".format(config('UAE_USERNAME'),config('UAE_PASSWORD'))
    response = requests.request("POST", url, headers={}, data = {})
    response_dict = json.loads(response.text)
    token = response_dict["access_token"]

    hasNextP = True
    allData = []
    newData = []
    cursor = ""
    retailerId = "1"
    new_url = "https://sssports.api.fluentretail.com/graphql"
    new_header = {'Authorization': 'Bearer '+token, 'Origin': 'https://production-eu01-sunandsand.demandware.net',
                  'Content-Type': ''}
    body = """{"query":\
      "query getReturnFulfilment ($ref: [String!]) {\
        returnFulfilments(ref:$ref,createdOn: {from: \\"2020-10-19\\"},first: 100) {\
          pageInfo { hasNextPage  hasPreviousPage } edges {\
            cursor node  { ref, type, returnOrder {  ref,  id,  order { ref }  id,  ref }\
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
          "variables":{"retailerId":"2","includeReturnOrderItems":false,"includePickupLocation":false,"includeAttributes":true,"includeFulfilment":true}\
      }
    """


    response = requests.post(new_url, headers=new_header, data=body)
    data = response.json()
    print(data)
    return data