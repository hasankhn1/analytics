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
crontab = Crontab(app)
log = logging.getLogger()

api = Api(app)
api.add_resource(Item, '/stores')

scheduler = APScheduler()
def scheduled_task():
  os.system('python ksa_report.py')
  os.system('python uae_report.py')
  os.system('python kuwait_report.py')

def scheduled_analytics():
  os.system('python analytics.py')

if __name__ == '__main__':
  scheduler.add_job(id='Scheduled Orders', func=scheduled_task, trigger='interval', minutes = 3)
  scheduler.add_job(id='Analytics', func=scheduled_analytics, trigger='interval', minutes = 4)
  scheduler.start()
  app.run(port=5000, debug=True)
