from google.cloud import bigquery
import pandas
import pytz

import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=".\shape-346218-a42d395b207c.json"


project_id = 'shape-346218'

client = bigquery.Client()

print("Reading equipment_sensors.csv ...")
f = pandas.read_csv("equipment_sensors.csv", sep=';')
equipment_sensors = 'exercise.equipment_sensors'

job_equipment_sensors = client.load_table_from_dataframe(
    f, equipment_sensors
)
job_equipment_sensors.result()

print("Reading equipment.csv ...")
j = pandas.read_json("equipment.json")
equipment = 'exercise.equipment'
job_equipment = client.load_table_from_dataframe(
    j, equipment
)
job_equipment.result()

print("Reading equipment_failure_sensors.csv ...")
l = pandas.read_csv("equipment_failure_sensors.log", names=['sensor'], sep=";")
l['date']=l['sensor'].str.extract(r'\[(.*)\]\t.*')
l['date']=l['date'].apply(pandas.Timestamp)
l['sensor']=l['sensor'].str.extract(r'.*sensor\[([0-9]*)\].*')
equipment_failure_sensors = 'exercise.equipment_failure_sensors'
job_equipment_failure_sensors = client.load_table_from_dataframe(
    l, equipment_failure_sensors
)
job_equipment_failure_sensors.result()