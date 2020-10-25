import os
from flask import Flask, request
from flask_apscheduler import APScheduler
from gevent.pywsgi import  WSGIServer
from decouple import config
import logging
from contextlib import suppress

app = Flask(__name__)
scheduler = APScheduler()
log = logging.getLogger()


def scheduled_ksa():
  print('hello')
  import ksa_report


def scheduled_uae():
  import uae_report


def scheduled_kuwait():
  import kuwait_report


def scheduled_analytics():
  os.system('python anlaytics.py')

if __name__ == '__main__':
  if config('ENVIRONEMENT') == 'development':
    log.warning("Running on debug mode not for production.")
    app.run(host='127.0.0.1', port=5000, debug=True)
  else:
    http_server = WSGIServer(('127.0.0.1', 5000), app)
    log.warning("Running on  production.")
    with suppress(KeyboardInterrupt):
      http_server.serve_forever()