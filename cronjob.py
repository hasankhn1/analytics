from apscheduler.schedulers.blocking import BlockingScheduler

# Main cronjob function.
from main import ksa_job,uae_job,kuwaitjob

# Create an instance of scheduler and add function.
scheduler = BlockingScheduler()
scheduler.add_job(ksa_job, "interval", seconds=30)
scheduler.add_job(uae_job, "interval", seconds=30)
scheduler.add_job(kuwaitjob, "interval", seconds=3)

scheduler.start()