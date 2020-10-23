import os
from flask import Flask, request
from flask_apscheduler import APScheduler

app = Flask(__name__)
scheduler = APScheduler()


def scheduled_ksa():
  os.system('python ksa_report.py')


def scheduled_uae():
  os.system('python uae_report.py')


def scheduled_kuwait():
  os.system('python kuwait_report.py')


def scheduled_analytics():
  os.system('python analytics.py')


if __name__ == '__main__':
  scheduler.add_job(id='Scheduled KSA Orders', func=scheduled_ksa,
                    trigger='interval', seconds=10)
  # scheduler.add_job(id='Scheduled UAE Orders', func=scheduled_uae,
  #                   trigger='interval', seconds=3)
  # scheduler.add_job(id='Scheduled KUWAIT Orders', func=scheduled_kuwait,
  #                   trigger='interval', seconds=4)
  # scheduler.add_job(id='Analytics', func=scheduled_analytics,
  #                   trigger='interval', minutes=5)
  scheduler.start()
  app.run(port=5000, debug=True)
