import os
from flask import Flask, request
from flask_apscheduler import APScheduler

app = Flask(__name__)
scheduler = APScheduler()


def scheduled_ksa():
  print('hello')
  import ksa_report


def scheduled_uae():
  import uae_report


def scheduled_kuwait():
  import kuwait_report


def scheduled_analytics():
  import anlaytics


if __name__ == '__main__':
  scheduler.add_job(id='Scheduled KSA Orders', func=scheduled_ksa,
                    trigger='interval', seconds=13)
  scheduler.add_job(id='Scheduled UAE Orders', func=scheduled_uae,
                    trigger='interval', minutes=10)
  scheduler.add_job(id='Scheduled KUWAIT Orders', func=scheduled_kuwait,
                    trigger='interval', minutes=10)
  scheduler.add_job(id='Analytics', func=scheduled_analytics,
                    trigger='interval', minutes=12)
  scheduler.start()
  app.run(port=5000, debug=True)
