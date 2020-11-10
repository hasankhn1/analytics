import os
from flask import Flask, request
from flask_apscheduler import APScheduler
from gevent.pywsgi import WSGIServer
from decouple import config
import logging
from contextlib import suppress
from flask_restful import Resource, Api, reqparse
from getReq import  Item

app = Flask(__name__)
log = logging.getLogger()

api = Api(app)
api.add_resource(Item, '/stores')

scheduler = APScheduler()
def sfcc_task():
  print('running')
  os.system('python ksa_report.py')
  os.system('python uae_report.py')
  os.system('python kuwait_report.py')
  # os.system('python uae_test.py')


def fluent_task():
  print('running2')
  os.system('python consignments.py')
  os.system('python fulfilments.py')
  os.system('python return_fulfilments.py')
  os.system('python return_order.py')
  

def sfcc_analytics():
  os.system('python sfcc_analytics.py')



def fluent_analytics():
  os.system('python fluent_analytics.py')


if __name__ == '__main__':

  scheduler.add_job(id='sfcc', func=sfcc_task, trigger='interval', minutes = 8)
  scheduler.add_job(id='fluent', func=fluent_task, trigger='interval', minutes = 13)
  scheduler.add_job(id='sfcc_analytics', func=sfcc_analytics, trigger='interval', minutes = 18)
  scheduler.add_job(id='fluent_analytics', func=fluent_analytics, trigger='interval', minutes = 30)
  scheduler.start()
  app.run(port=5000, use_reloader=False)
