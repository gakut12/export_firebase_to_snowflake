from google.cloud import bigquery
from datetime import datetime, timedelta, timezone
import sys
import os

bucket_name = os.environ['BUCKET_NAME']
extract_dir_name = "firebase_analytics";
project_id = os.environ['PROJECT_ID']
dataset_name = os.environ['DATASET_NAME']

# slack alert mail 
SLACK_WEBHOOK_URL = 'https://hooks.slack.com/services/'\
                    '************/**********************'
def extract_firebase2gcs(request):
    # execute 1 day ago data
    return _extract_firebase2gcs(1)

def _extract_firebase2gcs(dayago):
    if dayago is None:
        dayago = 1

    JST = timezone(timedelta(hours=+9), 'JST')
    today = datetime.now(JST)

    target_date = today - timedelta(days=int(dayago))
    target_date_str = datetime.strftime(target_date, '%Y%m%d')
    print("target_date -> " + target_date_str)


    query = ("EXPORT DATA OPTIONS("
             "uri='gs://{}/{}/events-{}-*',"
             "format='PARQUET',"
             "compression='SNAPPY',"
             "overwrite=true"
             ") AS "
             "SELECT * FROM {}.{}.events_{};").format(bucket_name,
                                                      extract_dir_name,
                                                      target_date_str,
                                                      project_id, dataset_name,
                                                      target_date_str)

    client = bigquery.Client()
    try:
        print(query)
        query_job = client.query(query)
        query_job.result()
        return "OK"
    except Exception as e:
        raise e
        # alert to slack
        print("firebase export failed!")
        requests.post(SLACK_WEBHOOK_URL,
                  data=json.dumps({
                      "text":
                      "firebase export failed!"
                  }))
        return "NG"

if __name__ == "__main__":
    arg = None
    if len(sys.argv) >= 2:
        arg = sys.argv[1]
    _extract_firebase2gcs(arg)
