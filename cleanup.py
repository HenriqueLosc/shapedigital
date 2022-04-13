from google.cloud import bigquery
import pandas
import pytz

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=".\shape-346218-a42d395b207c.json"
print("Cleaning up tables...")

project_id = 'shape-346218'

client = bigquery.Client()

cleanup = """
    DELETE `shape-346218.exercise.equipment_failure_sensors`
    WHERE TRUE
"""
client.query(cleanup, project=project_id)
cleanup = """
    DELETE `shape-346218.exercise.equipment`
    WHERE TRUE
"""
client.query(cleanup, project=project_id)
cleanup = """
    DELETE `shape-346218.exercise.equipment_sensors`
    WHERE TRUE
"""
client.query(cleanup, project=project_id)