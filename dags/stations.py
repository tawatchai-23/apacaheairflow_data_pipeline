from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
import requests
import xml.etree.ElementTree as ET
import psycopg2
import logging
import pendulum
from datetime import datetime, timedelta

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

    # URL ของ API
    url = "https://data.tmd.go.th/api/Station/v1/?uid=demo&ukey=demokey"

    # ดึงข้อมูล XML จาก API
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data: {response.status_code}")
    logging.info("Fetched XML data from TMD API")
    return response.text  # ส่ง XML string

def transform_data(**context):
    xml_data = context["ti"].xcom_pull(task_ids="extract_data")
    root = ET.fromstring(xml_data)

    # ดึงข้อมูลจาก <header>
    header = root.find("header")
    header_data = {
        "title": header.findtext("title"),
        "description": header.findtext("description"),
        "uri": header.findtext("uri"),
        "last_build_date": header.findtext("lastBuildDate"),
        "copyright": header.findtext("copyRight"),
        "generator": header.findtext("generator"),
        "status": header.findtext("status")
    }

    # ดึงข้อมูลจาก <Station>
    stations = []
    for station in root.findall("Station"):
        # Helper function: ดึงค่าและแปลงเป็น float
        def get_value_and_unit(element, default_value=None):
            if element is not None:
                return float(element.text) if element.text else default_value
            return default_value

        station_data = {
            "StationID": station.findtext("StationID"),
            "WmoCode": station.findtext("WmoCode"),
            "StationNameThai": station.findtext("StationNameThai"),
            "StationNameEnglish": station.findtext("StationNameEnglish"),
            "StationType": station.findtext("StationType"),
            "Province": station.findtext("Province"),
            "ZipCode": station.findtext("ZipCode"),
            "Latitude": float(station.findtext("Latitude")) if station.findtext("Latitude") else None,
            "Longitude": float(station.findtext("Longitude")) if station.findtext("Longitude") else None,
            "HeightAboveMSL": get_value_and_unit(station.find("HeightAboveMSL"), None),
            "HeightofWindWane": get_value_and_unit(station.find("HeightofWindWane"), None),
            "HeightofBarometer": get_value_and_unit(station.find("HeightofBarometer"), None),
            "HeightofThermometer": get_value_and_unit(station.find("HeightofThermometer"), None),
            # ข้อมูลจาก <header>
            "HeaderTitle": header_data["title"],
            "HeaderDescription": header_data["description"],
            "HeaderUri": header_data["uri"],
            "HeaderLastBuildDate": header_data["last_build_date"],
            "HeaderCopyRight": header_data["copyright"],
            "HeaderGenerator": header_data["generator"],
            "HeaderStatus": header_data["status"]
        }
        stations.append(station_data)
    return stations  # ✅ ต้องคืนค่ากลับเพื่อให้ load_data ใช้ xcom_pull ได้
        
def load_data(**context):
    data_list = context["ti"].xcom_pull(task_ids="transform_data")
    if not data_list:
        logging.warning("No data to load.")
        return
    # เชื่อมต่อ PostgreSQL
    conn = psycopg2.connect(
        host="172.27.154.25",
        database="othersource",  # แก้ไขชื่อฐานข้อมูล
        user="gi.joke",  # แก้ไขชื่อผู้ใช้
        password="Tawatcha1@2021"  # แก้ไขรหัสผ่าน
    )
    cur = conn.cursor()

    # แทรกข้อมูลลงใน PostgreSQL
    for station in data_list:
        sql = """
        INSERT INTO tmd.stations_airflow (
            title, description, uri, last_build_date, copyright, generator, status,
            station_id, wmo_code, station_name_thai, station_name_english, station_type, 
            province, zip_code, latitude, longitude, geom, height_above_msl, height_of_wind_vane,
            height_of_barometer, height_of_thermometer
        ) VALUES (
            %(HeaderTitle)s, %(HeaderDescription)s, %(HeaderUri)s, %(HeaderLastBuildDate)s, 
            %(HeaderCopyRight)s, %(HeaderGenerator)s, %(HeaderStatus)s,
            %(StationID)s, %(WmoCode)s, %(StationNameThai)s, %(StationNameEnglish)s, %(StationType)s,
            %(Province)s, %(ZipCode)s, %(Latitude)s, %(Longitude)s, 
            ST_SetSRID(ST_MakePoint(%(Longitude)s, %(Latitude)s), 4326), 
            %(HeightAboveMSL)s, %(HeightofWindWane)s, %(HeightofBarometer)s, %(HeightofThermometer)s
        )
        """
        cur.execute(sql, station)

    # Commit และปิดการเชื่อมต่อ
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # 'on_failure_callback': discord_alert,
}    

with DAG(
    "tmd_station_etl_dag",
    default_args= default_args,
    description="ETL TMD WeatherToday XML to PostgreSQL",
    schedule_interval="30 7 * * *",  # เวลา 07:30 ทุกวัน
    start_date=timezone.datetime(2025, 6, 4, 7, 30, tzinfo=local_tz),
    catchup=False,
    tags=["tmd", "weather", "etl"],
    on_failure_callback=discord_alert,     # แจ้งเตือนเมื่อ DAG ล้มเหลว
    on_success_callback=discord_success    # แจ้งเตือนเมื่อ DAG สำเร็จ
) as dag:
    
    def my_task():
        raise Exception("This is a test error!")  # จำลอง error
    
    start = EmptyOperator(task_id="start")
    
    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,   
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )
    t3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    ) 

    end = EmptyOperator(task_id="end")

    
    start >> t1 >> t2 >> t3 >> end
