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

# ฟังก์ชันสำหรับแปลง string เป็น float อย่างปลอดภัย
def safe_float(value):
    try:
        return float(value.strip())
    except (ValueError, AttributeError):
        return None

def extract_data():
    # URL API
    url = "https://data.tmd.go.th/api/RainRegions/v1/?uid=api&ukey=api12345"
    # ดึงข้อมูล XML จาก API
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to fetch data: {response.status_code}")
    logging.info("Fetched XML data from TMD API")
    return response.text


def transform_data(**context):
    xml_data = context["ti"].xcom_pull(task_ids="extract_data")
    root = ET.fromstring(xml_data)

    header = root.find(".//Header")
    header_data = {
        "title": header.findtext("Title"),
        "description": header.findtext("Description"),
        "uri": header.findtext("Uri"),
        "last_build_date": header.findtext("LastBuildDate"),
        "date_of_data": header.findtext("DateOfData"),
        "copyright": header.findtext("CopyRight"),
        "generator": header.findtext("Generator"),
        "status": header.findtext("status"),
    }

    results = []
    for region in root.findall(".//Region"):
        region_name = region.findtext("RegionName")
        for province in region.findall(".//Province"):
            province_name = province.findtext("ProvinceName")
            for station in province.findall(".//Station"):
                station_data = {
                    **header_data,
                    "region_name": region_name,
                    "province_name": province_name,
                    "station_name": station.findtext("StationNameThai"),
                    "latitude": safe_float(station.find("Latitude").text),
                    "longitude": safe_float(station.find("Longitude").text),
                    "rainfall": safe_float(station.find("Rainfall").text),
                }
                results.append(station_data)

    return results


def load_data(**context):
    data_list = context["ti"].xcom_pull(task_ids="transform_data")
    if not data_list:
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

    for row in data_list:
        cur.execute("""
            INSERT INTO tmd.rainregions_airflow (
                title, description, uri, last_build_date, date_of_data,
                copyright, generator, status,
                region_name, province_name, station_name,
                latitude, longitude, rainfall
            ) VALUES (
                %(title)s, %(description)s, %(uri)s, %(last_build_date)s, %(date_of_data)s,
                %(copyright)s, %(generator)s, %(status)s,
                %(region_name)s, %(province_name)s, %(station_name)s,
                %(latitude)s, %(longitude)s, %(rainfall)s
            )
        """, row)

    conn.commit()
    cur.close()
    conn.close()
    logging.info("Data loaded to PostgreSQL successfully.")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": discord_alert  # 👈 เพิ่มตรงนี้
}

with DAG(
    "tmd_rainregions_etl_dag",
    default_args=default_args,
    description="ETL TMD RainRegions XML to PostgreSQL",
    schedule_interval="30 7 * * *",  # เวลา 07:30 ทุกวัน
    start_date=timezone.datetime(2025, 6, 4, 7, 30, tzinfo=local_tz),
    tags=["tmd", "rain", "etl"],
    catchup=False,
    on_failure_callback=discord_alert,     # แจ้งเตือนเมื่อ DAG ล้มเหลว
    on_success_callback=discord_success,    # แจ้งเตือนเมื่อ DAG สำเร็จ
) as dag:
    start = EmptyOperator(task_id="start")

    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    start >> t1 >> t2 >> t3 >> end
