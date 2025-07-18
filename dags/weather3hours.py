from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from datetime import datetime, timedelta
import logging
import requests
import xml.etree.ElementTree as ET
import psycopg2
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

def safe_float(value, default=0.0):
    try:
        return float(value.strip()) if value and value.strip() else default
    except ValueError:
        return default

def extract_weather_data(**context):
    url = "https://data.tmd.go.th/api/Weather3Hours/V2/?uid=api&ukey=api12345"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data: {response.status_code}")
    return response.text

def transform_weather_data(**context):
    xml_data = context["ti"].xcom_pull(task_ids="extract_weather_data")
    root = ET.fromstring(xml_data)

    header_element = root.find("Header")
    header_data = {
        "title": header_element.findtext("Title"),
        "description": header_element.findtext("Description"),
        "uri": header_element.findtext("Uri"),
        "last_build_date": header_element.findtext("LastBuildDate"),
        "copyright": header_element.findtext("CopyRight"),
        "generator": header_element.findtext("Generator"),
        "status": header_element.findtext("status"),
    }

    rows = []
    for station_element in root.findall(".//Station"):
        observation_element = station_element.find("Observation")
        row = {
            **header_data,
            "wmo_station_number": station_element.findtext("WmoStationNumber"),
            "station_name_thai": station_element.findtext("StationNameThai"),
            "station_name_english": station_element.findtext("StationNameEnglish"),
            "province": station_element.findtext("Province"),
            "latitude": safe_float(station_element.findtext("Latitude")),
            "longitude": safe_float(station_element.findtext("Longitude")),
            "datetime": observation_element.findtext("DateTime"),
            "station_pressure": safe_float(observation_element.findtext("StationPressure")),
            "mean_sea_level_pressure": safe_float(observation_element.findtext("MeanSeaLevelPressure")),
            "minimum_temperature": safe_float(observation_element.findtext("MinimumTemperature")),
            "air_temperature": safe_float(observation_element.findtext("AirTemperature")),
            "dew_point": safe_float(observation_element.findtext("DewPoint")),
            "relative_humidity": safe_float(observation_element.findtext("RelativeHumidity")),
            "vapor_pressure": safe_float(observation_element.findtext("VaporPressure")),
            "land_visibility": safe_float(observation_element.findtext("LandVisibility")),
            "wind_direction": safe_float(observation_element.findtext("WindDirection")),
            "wind_speed": safe_float(observation_element.findtext("WindSpeed")),
            "rainfall": safe_float(observation_element.findtext("Rainfall")),
            "rainfall_24hr": safe_float(observation_element.findtext("Rainfall24Hr")),
        }
        rows.append(row)
    return rows

def load_weather_data(**context):
    rows = context["ti"].xcom_pull(task_ids="transform_weather_data")

    if not rows:
        logging.warning("No data to load.")
        return

    conn = psycopg2.connect(
        dbname="othersource",
        user="gi.joke",
        password="Tawatcha1@2021",
        host="172.27.154.25",
        port="5432"
    )
    cur = conn.cursor()

    insert_query = """
        INSERT INTO tmd.weather3hours_airflow (
            title, description, uri, last_build_date, copyright,
            generator, status, wmo_station_number, station_name_thai,
            station_name_english, province, latitude, longitude, datetime,
            station_pressure, mean_sea_level_pressure, minimum_temperature,
            air_temperature, dew_point, relative_humidity, vapor_pressure,
            land_visibility, wind_direction, wind_speed, rainfall, rainfall_24hr, geom
        ) VALUES (
            %(title)s, %(description)s, %(uri)s, %(last_build_date)s, %(copyright)s,
            %(generator)s, %(status)s, %(wmo_station_number)s, %(station_name_thai)s,
            %(station_name_english)s, %(province)s, %(latitude)s, %(longitude)s, %(datetime)s,
            %(station_pressure)s, %(mean_sea_level_pressure)s, %(minimum_temperature)s,
            %(air_temperature)s, %(dew_point)s, %(relative_humidity)s, %(vapor_pressure)s,
            %(land_visibility)s, %(wind_direction)s, %(wind_speed)s, %(rainfall)s, %(rainfall_24hr)s,
            ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)
        )
    """

    cur.executemany(insert_query, rows)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Data successfully loaded to PostgreSQL.")

# Default arguments
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": discord_alert  # 👈 เพิ่มตรงนี้
}

# DAG definition
with DAG(
    "tmd_weather3hours_etl",
    default_args=default_args,
    description="ETL TMD Weather 3-Hour Forecast into PostgreSQL",
    schedule_interval="0 */3 * * *",  # ปรับตามความต้องการ เช่น ทุก 1 ชั่วโมง
    start_date=datetime(2025, 6, 4, 1, 30, tzinfo=local_tz),
    # catchup=False,
    tags=["tmd", "weather3hours", "etl"],
    on_failure_callback=discord_alert,     # แจ้งเตือนเมื่อ DAG ล้มเหลว
    on_success_callback=discord_success    # แจ้งเตือนเมื่อ DAG สำเร็จ
) as dag:
    
    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data
    )

    t2 = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data
    )

    t3 = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data
    )

    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> end
