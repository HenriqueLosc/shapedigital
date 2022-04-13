from google.cloud import bigquery
import pandas
import pytz
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=".\shape-346218-a42d395b207c.json"

project_id = 'shape-346218'


from google.cloud import bigquery
print("Processing results...")

client = bigquery.Client()
sql_total_failures = """
    SELECT COUNT(*) as total_failures FROM `shape-346218.exercise.equipment_failure_sensors`
    WHERE DATE(date) >= '2020-01-01' AND DATE(date) <= '2020-01-31'
"""
df_total_failures = client.query(sql_total_failures, project=project_id).to_dataframe()
print("\n\n1 – Total equipment failures that happened?\n")
print(df_total_failures)
print()


sql_most_failures = """
SELECT
  code,
  SUM(total_failures) AS total_failures
FROM (
  SELECT
    sensor,
    COUNT(*) AS total_failures
  FROM
    `shape-346218.exercise.equipment_failure_sensors`
  WHERE
    DATE(date) BETWEEN '2020-01-01'
    AND '2020-01-31'
  GROUP BY
    sensor) failures
JOIN (
  SELECT
    *
  FROM
    `shape-346218.exercise.equipment_sensors`) equipments
ON
  CAST(failures.sensor AS INT64) = equipments.sensor_id
JOIN (
  SELECT
    *
  FROM
    `shape-346218.exercise.equipment`) codes
ON
  codes.equipment_id = equipments.equipment_id
GROUP BY
  code
ORDER BY
  total_failures DESC
LIMIT 1
"""
df_most_failures = client.query(sql_most_failures, project=project_id).to_dataframe()
print("\n\n2 – Which equipment code had most failures?\n")
print(df_most_failures)
print()



sql_average_failures = """
SELECT
  group_name,
  ROUND(AVG(total_failures),2) AS avg_failures,
  SUM(total_failures) AS total_failures
FROM (
  SELECT
    sensor,
    COUNT(*) AS total_failures
  FROM
    `shape-346218.exercise.equipment_failure_sensors`
  WHERE
    DATE(date) BETWEEN '2020-01-01'
    AND '2020-01-31'
  GROUP BY
    sensor) failures
JOIN (
  SELECT
    *
  FROM
    `shape-346218.exercise.equipment_sensors`) equipments
ON
  CAST(failures.sensor AS INT64) = equipments.sensor_id
JOIN (
  SELECT
    *
  FROM
    `shape-346218.exercise.equipment`) codes
ON
  codes.equipment_id = equipments.equipment_id
GROUP BY
  group_name
ORDER BY
  total_failures
"""
df_average_failures = client.query(sql_average_failures, project=project_id).to_dataframe()
print("\n\n3 – Average amount of failures across equipment group, ordered by the number of failures in ascending order?\n")
print(df_average_failures)
print()
