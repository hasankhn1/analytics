import os
from flask import Flask, request
from flask_apscheduler import APScheduler
from gevent.pywsgi import WSGIServer
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


def run_jobs():
  scheduler.add_job(id='Scheduled KSA Orders', func=scheduled_ksa,
                    trigger='interval', seconds=5)
  scheduler.add_job(id='Scheduled UAE Orders', func=scheduled_uae,
                    trigger='interval', minutes=10)
  scheduler.add_job(id='Scheduled KUWAIT Orders', func=scheduled_kuwait,
                    trigger='interval', minutes=10)
  scheduler.add_job(id='Analytics', func=scheduled_analytics,
                    trigger='interval', minutes=12)
  scheduler.start()


if __name__ == '__main__':
  if config('ENVIRONMENT') == 'development':
    log.warning("Running on debug mode not for production.")
    run_jobs()
    app.run(host='127.0.0.1', port=5000, debug=True)
  else:
    http_server = WSGIServer(('127.0.0.1', 5000), app)
    log.warning("Running on  production.")
    run_jobs()
    with suppress(KeyboardInterrupt):
      http_server.serve_forever()
