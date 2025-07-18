from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from datetime import datetime, timedelta
import logging
import requests
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import RealDictCursor
import pendulum

local_tz = pendulum.timezone("Asia/Bangkok")

def discord_alert(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = f"""
🚨 **Airflow Alert**
DAG: `{dag_id}`
Task: `{task_id}`
Execution Time: `{execution_date}`
[View Logs]({log_url})
"""

    webhook_url = "https://discordapp.com/api/webhooks/1379017347674931200/XQVyCWJwO4tMTpxngx-7MKKXoqf9BGLwL5gnj85Rg9hpXcmEU0aE_9yQ627pUmlhuo_7"  # 🔁 แทนด้วย webhook ของคุณ

    payload = {
        "content": message
    }

    try:
        requests.post(webhook_url, json=payload)
    except Exception as e:
        print(f"Failed to send Discord alert: {e}")

def discord_success(context):
    dag_id = context.get("dag").dag_id
    execution_date = context.get("execution_date")

    message = (
        f"✅ DAG Success!\n"
        f"**DAG**: `{dag_id}`\n"
        f"**Time**: {execution_date}"
    )

    webhook_url = "https://discordapp.com/api/webhooks/1379017347674931200/XQVyCWJwO4tMTpxngx-7MKKXoqf9BGLwL5gnj85Rg9hpXcmEU0aE_9yQ627pUmlhuo_7"
    payload = {"content": message}
    requests.post(webhook_url, json=payload)

def extract_data(**context):
    url = "https://data.tmd.go.th/api/WeatherToday/V2/?uid=api&ukey=api12345"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data: {response.status_code}")
    logging.info("Fetched XML data from TMD API")
    return response.text  # ส่ง XML string

def transform_data(**context):
    xml_data = context["ti"].xcom_pull(task_ids="extract_data")
    root = ET.fromstring(xml_data)

    header = root.find(".//Header")
    header_data = {
        "title": header.findtext("Title"),
        "description": header.findtext("Description"),
        "uri": header.findtext("Uri"),
        "last_build_date": header.findtext("LastBuildDate"),
        "copyright": header.findtext("CopyRight"),
        "generator": header.findtext("Generator"),
        "status": header.findtext("status")
    }

    def safe_float(value):
        try:
            return float(value.strip()) if value else None
        except Exception:
            return None

    data_list = []
    for station in root.findall(".//Station"):
        obs = station.find("Observation")
        record = {
            **header_data,
            "station_number": station.findtext("WmoStationNumber"),
            "station_name_thai": station.findtext("StationNameThai"),
            "station_name_english": station.findtext("StationNameEnglish"),
            "province": station.findtext("Province"),
            "latitude": safe_float(station.findtext("Latitude")),
            "longitude": safe_float(station.findtext("Longitude")),
            "observation_datetime": obs.findtext("DateTime"),
            "mean_sea_level_pressure": safe_float(obs.findtext("MeanSeaLevelPressure")),
            "temperature": safe_float(obs.findtext("Temperature")),
            "max_temperature": safe_float(obs.findtext("MaxTemperature")),
            "diff_from_max_temperature": safe_float(obs.findtext("DifferentFromMaxTemperature")),
            "min_temperature": safe_float(obs.findtext("MinTemperature")),
            "diff_from_min_temperature": safe_float(obs.findtext("DifferentFromMinTemperature")),
            "relative_humidity": safe_float(obs.findtext("RelativeHumidity")),
            "wind_direction": safe_float(obs.findtext("WindDirection")),
            "wind_speed": safe_float(obs.findtext("WindSpeed")),
            "rainfall": safe_float(obs.findtext("Rainfall"))
        }
        data_list.append(record)

    logging.info(f"Transformed {len(data_list)} records.")
    return data_list

def load_data(**context):
    data_list = context["ti"].xcom_pull(task_ids="transform_data")
    if not data_list:
        logging.warning("No data to load.")
        return

    conn = psycopg2.connect(
        dbname="othersource",
        user="gi.joke",
        password="Tawatcha1@2021",
        host="172.27.154.25"
    )
    cur = conn.cursor(cursor_factory=RealDictCursor)

    insert_sql = """
    INSERT INTO tmd.weathertoday_airflow (
        title, description, uri, last_build_date, copyright,
        generator, status, station_number, station_name_thai,
        station_name_english, province, latitude, longitude,
        observation_datetime, mean_sea_level_pressure, temperature,
        max_temperature, diff_from_max_temperature, min_temperature,
        diff_from_min_temperature, relative_humidity, wind_direction,
        wind_speed, rainfall
    ) VALUES (
        %(title)s, %(description)s, %(uri)s, %(last_build_date)s, %(copyright)s,
        %(generator)s, %(status)s, %(station_number)s, %(station_name_thai)s,
        %(station_name_english)s, %(province)s, %(latitude)s, %(longitude)s,
        %(observation_datetime)s, %(mean_sea_level_pressure)s, %(temperature)s,
        %(max_temperature)s, %(diff_from_max_temperature)s, %(min_temperature)s,
        %(diff_from_min_temperature)s, %(relative_humidity)s, %(wind_direction)s,
        %(wind_speed)s, %(rainfall)s
    )
    """

    for record in data_list:
        cur.execute(insert_sql, record)

    conn.commit()
    conn.close()
    logging.info(f"Inserted {len(data_list)} records into PostgreSQL.")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": discord_alert  # 👈 เพิ่มตรงนี้
}

with DAG(
    "tmd_weather_today_etl_dag",
    default_args= default_args,
    description="ETL TMD WeatherToday XML to PostgreSQL",
    schedule_interval="30 7 * * *",  # เวลา 07:30 ทุกวัน
    start_date=datetime(2025, 6, 4, 7, 30, tzinfo=local_tz),
    catchup=False,
    tags=["tmd", "weather", "etl"],
    on_failure_callback=discord_alert,     # แจ้งเตือนเมื่อ DAG ล้มเหลว
    on_success_callback=discord_success    # แจ้งเตือนเมื่อ DAG สำเร็จ
) as dag:
    
    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    t3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> end
