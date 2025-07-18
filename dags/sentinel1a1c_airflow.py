from __future__ import annotations
import os
import pendulum
import requests
import psycopg2
from datetime import datetime, timedelta
import json
import pytz
from shapely.geometry import shape, box
from psycopg2.extras import Json # Keep this for reference, not directly used for geometry type here
from minio import Minio

from psycopg2 import sql # Import sql module for safer SQL string composition
from psycopg2.extras import execute_values # Import execute_values explicitly
from psycopg2.extensions import AsIs # <--- เพิ่มการ import นี้

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin # For consistent logging

# Configure logging for the DAG
log = LoggingMixin().log

# ====== CONFIGURATION ======
# Spectator Earth API
SPECTATOR_API_KEY = "Moe2TxQzUYHab47hzXiWEJ"
SPECTATOR_API_URL = "https://api.spectator.earth/acquisition-plan/"

# PostgreSQL Connection Details (using direct variables as requested)
PG_DBNAME = "aq_plan"
PG_USER = "gi.joke"
PG_PASSWORD = "Tawatcha1@2021"
PG_HOST = "172.27.154.25"
PG_PORT = "5432"

# Target table and satellite names
TABLE_NAME = "acquisition.sentinel1"
SATELLITES_TO_FETCH = "Sentinel-1A,Sentinel-1C" # As a comma-separated string for the API
SATELLITES_FOR_DELETION = ["Sentinel-1A", "Sentinel-1C"]  # Specific satellite for deletion logic

# MinIO Staging Configuration
MINIO_URL = "172.27.127.90:9000" # Use the same MinIO URL as your previous DAG
MINIO_ACCESS_KEY = "08RJAwKtz8QM8hRlVLY2" # Your access key
MINIO_SECRET_KEY = "kunOgQyMLnM6tqq7pxUY8jDMWuvKuuW04BA7Nt0j" # Your secret key
STAGING_BUCKET_NAME = "gi-disaster" # A new bucket specifically for staging data
STAGING_PREFIX = "sentinel1_acquisition/"

# Bounding box for Thailand (WGS84 - EPSG:4326)
THAILAND_BBOX = box(97.343807146, 5.6128510107, 105.6368118525, 20.464833874)

# Timezone for Thailand
THAILAND_TIMEZONE = pytz.timezone('Asia/Bangkok')

# ====== HELPER FUNCTIONS ======
def _get_pg_connection():
    return psycopg2.connect(
        dbname=PG_DBNAME,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

def _get_minio_client():
    return Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

# ====== ETL TASKS ======

def _delete_old_data(**kwargs):
    # ดึง data_interval_start ซึ่งเป็นวันเริ่มต้นของการรัน DAG นี้
    # และใช้เป็นจุดเริ่มต้นสำหรับการลบข้อมูล (ตั้งแต่วันนี้เป็นต้นไป)
    # เช่น ถ้า DAG รันวันที่ 4 ก.ค. 2025, delete_from_date จะเป็น 2025-07-04 00:00:00 TH
    delete_from_date = kwargs['data_interval_start'].in_timezone(THAILAND_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    
    conn = None
    cursor = None
    try:
        conn = _get_pg_connection()
        cursor = conn.cursor()
        
        # ใช้ sql.SQL เพื่อสร้างส่วนของ IN clause อย่างปลอดภัยและถูกต้อง
        in_clause = sql.SQL("IN ({})").format(
            sql.SQL(', ').join(map(sql.Literal, SATELLITES_FOR_DELETION))
        )
        
        # สร้าง delete_query เพื่อลบข้อมูลตั้งแต่วันที่ delete_from_date เป็นต้นไป (>=)
        # สำหรับดาวเทียมที่ระบุใน SATELLITES_FOR_DELETION
        delete_query = sql.SQL('''
            DELETE FROM {}
            WHERE begin_time >= %s AND satellite_name {};
        ''').format(
            sql.Identifier(TABLE_NAME.split('.')[0], TABLE_NAME.split('.')[1]),
            in_clause
        )
        
        # Log the actual query for debugging
        log.info(f"Executing delete query: {cursor.mogrify(delete_query, (delete_from_date,)).decode('utf-8')}")

        # ส่งพารามิเตอร์: วันที่เริ่มต้นการลบ
        cursor.execute(delete_query, (delete_from_date,))
        
        conn.commit()
        log.info(f"ลบข้อมูลเก่าสำหรับ {', '.join(SATELLITES_FOR_DELETION)} ตั้งแต่ {delete_from_date.strftime('%Y-%m-%d')} เป็นต้นไป สำเร็จแล้ว")
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        log.error(f"❌ เกิดข้อผิดพลาดในการลบข้อมูลเก่า: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def _extract_and_transform_data(**kwargs):
    # ดึง data_interval_start ซึ่งเป็นวันเริ่มต้นของการรัน DAG นี้ (เช่น 4 ก.ค. 2025)
    # และใช้เป็นจุดเริ่มต้นสำหรับการวนลูปดึงข้อมูล
    start_date_for_fetch = kwargs['data_interval_start'].in_timezone(THAILAND_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    
    all_filtered_records = []
    coordinates_set = set()

    for i in range(11): # วนลูป 11 ครั้ง (i = 0 ถึง 10)
        # target_date จะเริ่มจาก start_date_for_fetch (วันนี้) ไปจนถึง 10 วันถัดไป
        # ตัวอย่าง: ถ้า start_date_for_fetch คือ 4 ก.ค.
        # i=0 -> 4 ก.ค.
        # i=1 -> 5 ก.ค.
        # ...
        # i=10 -> 14 ก.ค.
        target_date = start_date_for_fetch + timedelta(days=i)
        api_date_str = target_date.strftime("%Y-%m-%dT00:00:00")

        log.info(f"ดึงข้อมูลสำหรับวันที่: {api_date_str}")

        params = {
            "api_key": SPECTATOR_API_KEY,
            "datetime": api_date_str,
            "satellites": SATELLITES_TO_FETCH,
        }

        try:
            response = requests.get(SPECTATOR_API_URL, params=params, timeout=30)
            response.raise_for_status()
            api_response_features = response.json().get('features', [])
            log.info(f"ได้รับ {len(api_response_features)} features จาก API สำหรับ {api_date_str}")

            daily_filtered_features = []
            for feature in api_response_features:
                if 'geometry' in feature and feature['geometry'] is not None:
                    try:
                        geom_shapely = shape(feature['geometry'])
                        if THAILAND_BBOX.intersects(geom_shapely):
                            daily_filtered_features.append(feature)
                    except Exception as e:
                        log.warning(f"Skipping feature due to invalid geometry: {e} for feature: {feature.get('properties', {}).get('datatake_id', 'N/A')}")
                else:
                    log.warning(f"Skipping feature with missing geometry: {feature.get('properties', {}).get('datatake_id', 'N/A')}")
            
            log.info(f"กรองแล้วได้ {len(daily_filtered_features)} features สำหรับประเทศไทยจาก {api_date_str}")

            for feature in daily_filtered_features:
                data_type = feature['geometry']['type']
                coordinates_json_str = json.dumps(feature['geometry'])

                if coordinates_json_str in coordinates_set:
                    log.debug(f"Skip duplicate geometry for datatake_id: {feature['properties'].get('datatake_id', 'N/A')}")
                    continue
                coordinates_set.add(coordinates_json_str)

                swath = feature['properties'].get('swath')
                datatake_id = feature['properties'].get('datatake_id')
                polarisation = feature['properties'].get('polarisation')
                
                orbit_absolute = int(feature['properties']['orbit_absolute']) if 'orbit_absolute' in feature['properties'] and feature['properties']['orbit_absolute'] is not None else None
                orbit_relative = int(feature['properties']['orbit_relative']) if 'orbit_relative' in feature['properties'] and feature['properties']['orbit_relative'] is not None else None


                begin_time_utc_str = feature['properties'].get('begin_time')
                end_time_utc_str = feature['properties'].get('end_time')

                begin_time_utc = datetime.strptime(begin_time_utc_str, "%Y-%m-%dT%H:%M:%SZ") if begin_time_utc_str else None
                end_time_utc = datetime.strptime(end_time_utc_str, "%Y-%m-%dT%H:%M:%SZ") if end_time_utc_str else None

                begin_time_thai = begin_time_utc.replace(tzinfo=pytz.utc).astimezone(THAILAND_TIMEZONE) if begin_time_utc else None
                end_time_thai = end_time_utc.replace(tzinfo=pytz.utc).astimezone(THAILAND_TIMEZONE) if end_time_utc else None
                
                satellite = feature['properties'].get('satellite')

                record = {
                    "type": data_type,
                    "coordinates": coordinates_json_str,
                    "swath": swath,
                    "datatake_id": datatake_id,
                    "polarisation": polarisation,
                    "orbit_absolute": orbit_absolute,
                    "orbit_relative": orbit_relative,
                    "begin_time": begin_time_thai.isoformat() if begin_time_thai else None,
                    "begin_time_nottimezone": begin_time_utc.isoformat() if begin_time_utc else None,
                    "end_time": end_time_thai.isoformat() if end_time_thai else None,
                    "end_time_nottimezone": end_time_utc.isoformat() if end_time_utc else None,
                    "satellite": satellite
                }
                all_filtered_records.append(record)

        except requests.exceptions.RequestException as e:
            log.error(f"❌ ข้อผิดพลาดในการเรียก API สำหรับวันที่ {api_date_str}: {e}")
        except json.JSONDecodeError as e:
            log.error(f"❌ ข้อผิดพลาดในการถอดรหัส JSON สำหรับวันที่ {api_date_str}: {e}")
        except Exception as e:
            log.error(f"❌ เกิดข้อผิดพลาดที่ไม่คาดคิดสำหรับวันที่ {api_date_str}: {e}", exc_info=True)

    if not all_filtered_records:
        log.warning("ไม่พบข้อมูลที่กรองแล้วสำหรับช่วงเวลาทั้งหมด. จะไม่มีข้อมูลถูกอัปโหลดไปยัง Staging.")
        kwargs['ti'].xcom_push(key='sentinel1_staging_key', value=None)
        return

    # --- Upload to MinIO Staging ---
    minio_client = _get_minio_client()
    try:
        if not minio_client.bucket_exists(STAGING_BUCKET_NAME):
            minio_client.make_bucket(STAGING_BUCKET_NAME)
            log.info(f"สร้าง Bucket '{STAGING_BUCKET_NAME}' สำหรับ Staging แล้ว")
    except Exception as e:
        log.error(f"❌ ไม่สามารถตรวจสอบ/สร้าง MinIO bucket '{STAGING_BUCKET_NAME}' ได้: {e}")
        raise

    execution_date_format = kwargs['ds_nodash']
    current_time_str = datetime.now().strftime("%Y%m%d%H%M%S")
    staging_file_key = f"{STAGING_PREFIX}{execution_date_format}/data_{current_time_str}.json"
    
    temp_file_path = f"/tmp/sentinel1_data_{current_time_str}.json"
    try:
        with open(temp_file_path, 'w', encoding='utf-8') as f:
            json.dump(all_filtered_records, f, ensure_ascii=False, indent=2)
        
        minio_client.fput_object(STAGING_BUCKET_NAME, staging_file_key, temp_file_path)
        log.info(f"อัปโหลดข้อมูล {len(all_filtered_records)} รายการไปยัง MinIO Staging: {STAGING_BUCKET_NAME}/{staging_file_key}")
        
        kwargs['ti'].xcom_push(key='sentinel1_staging_key', value=staging_file_key)
    
    except Exception as e:
        log.error(f"❌ ข้อผิดพลาดในการเขียน/อัปโหลดข้อมูลไปยัง MinIO Staging: {e}", exc_info=True)
        raise
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

    log.info(f"ETL: ดึงและแปลงข้อมูลสำเร็จ. ไฟล์ Staging Key: {staging_file_key}")


def _load_data_to_postgres(**kwargs):
    """
    Loads the transformed data from MinIO Staging into the PostgreSQL table.
    """
    staging_file_key = kwargs['ti'].xcom_pull(key='sentinel1_staging_key', task_ids='extract_and_transform_sentinel1_data')

    if not staging_file_key:
        log.warning("ไม่มี Staging Key จาก XCom. ไม่มีข้อมูลที่จะโหลดเข้า PostgreSQL.")
        return

    minio_client = _get_minio_client()
    conn = None
    cursor = None
    
    temp_download_path = f"/tmp/sentinel1_data_download_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    try:
        minio_client.fget_object(STAGING_BUCKET_NAME, staging_file_key, temp_download_path)
        log.info(f"ดาวน์โหลดไฟล์ Staging '{staging_file_key}' สำเร็จแล้ว")
        
        with open(temp_download_path, 'r', encoding='utf-8') as f:
            transformed_records = json.load(f)
        
        if not transformed_records:
            log.warning("ไฟล์ Staging ว่างเปล่า. ไม่มีข้อมูลที่จะโหลดเข้า PostgreSQL.")
            return

        conn = _get_pg_connection()
        cursor = conn.cursor()

        # 1. ระบุคอลัมน์ทั้งหมดที่จะแทรก
        columns = [
            'type', 'coordinates', 'swath', 'datatake_id', 'polarisation',
            'orbit_absolute', 'orbit_relative', 'begin_time', 'begin_time_nottimezone',
            'end_time', 'end_time_nottimezone', 'satellite', 'satellite_name','geom' # geom เป็นคอลัมน์ PostGIS
        ]

        # 2. สร้าง Query: นี่คือสิ่งที่ execute_values คาดหวัง
        #    - ใช้ sql.Identifier เพื่อระบุชื่อตารางและคอลัมน์อย่างปลอดภัย
        #    - ในส่วน VALUES ใช้เพียง '%s' ตัวเดียว เพราะ execute_values จะสร้าง
        #      วงเล็บและ %s ภายในวงเล็บให้เองตามจำนวนคอลัมน์ใน data_to_insert
        insert_query = sql.SQL('''
            INSERT INTO {} ({})
            VALUES %s
        ''').format(
            sql.Identifier(TABLE_NAME.split('.')[0], TABLE_NAME.split('.')[1]),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
        
        # 3. เตรียม data_to_insert:
        #    - สำหรับ geom_column: แปลง GeoJSON string ให้เป็น SQL Expression โดยใช้ AsIs
        #      ต้องแน่ใจว่า GeoJSON string ถูก Escape อย่างถูกต้องสำหรับ SQL literal
        #    - เพิ่มค่าสำหรับ satellite_name โดยใช้ record.get('satellite')
        data_to_insert = []
        for record in transformed_records:
            geojson_for_geom = record.get('coordinates')
            
            # --- สำคัญ: การจัดการ GeoJSON string สำหรับ SQL literal ---
            # เราจำเป็นต้อง Escape single quotes ภายใน GeoJSON string ด้วยการเปลี่ยนเป็นสองตัว (' -> '')
            # เพื่อให้ ST_GeomFromGeoJSON('%s') ทำงานได้อย่างถูกต้องเมื่อ %s ถูกแทนที่ด้วย string นั้น
            # json.dumps โดยปกติจะจัดการเรื่องนี้ให้แล้วสำหรับ JSON string แต่เพื่อความชัวร์
            # หากพบปัญหาอีก ให้ตรวจสอบว่า GeoJSON string ที่ส่งไปมี single quote ไหม และ Escape หรือไม่
            
            # สร้าง SQL expression string สำหรับ geom โดยตรง
            geom_sql_expression = f"ST_SetSRID(ST_GeomFromGeoJSON('{geojson_for_geom.replace("'", "''")}'), 4326)"
            
            # ใช้ AsIs เพื่อให้ psycopg2 ส่ง string นี้เป็น SQL expression
            geom_as_is = AsIs(geom_sql_expression)

            data_to_insert.append((
                record.get('type'),
                record.get('coordinates'), # สำหรับคอลัมน์ 'coordinates' (text/jsonb)
                record.get('swath'),
                record.get('datatake_id'),
                record.get('polarisation'),
                record.get('orbit_absolute'),
                record.get('orbit_relative'),
                datetime.fromisoformat(record['begin_time']) if record.get('begin_time') else None,
                datetime.fromisoformat(record['begin_time_nottimezone']) if record.get('begin_time_nottimezone') else None,
                datetime.fromisoformat(record['end_time']) if record.get('end_time') else None,
                datetime.fromisoformat(record['end_time_nottimezone']) if record.get('end_time_nottimezone') else None,
                record.get('satellite'),
                record.get('satellite'), # <--- เพิ่มค่าสำหรับ satellite_name ที่นี่
                geom_as_is # สำหรับคอลัมน์ 'geom' (PostGIS geometry)
            ))

        log.info(f"กำลังแทรก {len(data_to_insert)} รายการเข้าสู่ {TABLE_NAME}...")
        
        # 4. เรียกใช้ execute_values ด้วย insert_query ที่ถูกต้อง
        execute_values(cursor, insert_query, data_to_insert, page_size=1000)
        
        conn.commit()
        log.info(f"✅ โหลดข้อมูลสำเร็จแล้ว: {len(data_to_insert)} รายการถูกเพิ่ม.")

    except Exception as e:
        if conn:
            conn.rollback()
        log.error(f"❌ ข้อผิดพลาดในการโหลดข้อมูลเข้า PostgreSQL หรือดาวน์โหลดจาก MinIO: {e}", exc_info=True)
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if os.path.exists(temp_download_path):
            os.remove(temp_download_path)
        
        # Optional: Clean up staging file from MinIO
        # try:
        #     if staging_file_key:
        #         minio_client.remove_object(STAGING_BUCKET_NAME, staging_file_key)
        #         log.info(f"ลบไฟล์ Staging '{staging_file_key}' ออกจาก MinIO แล้ว.")
        # except Exception as e:
        #     log.warning(f"⚠️ ไม่สามารถลบไฟล์ Staging '{staging_file_key}' ออกจาก MinIO ได้: {e}")


# ====== AIRFLOW DAG DEFINITION ======

with DAG(
    dag_id="import-sentinel1A-1C-from-api-taskflow",
    schedule="0 7 * * *", 
    # start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    # start_date=pendulum.datetime(2025, 7, 4, tz="Asia/Bangkok"),
    start_date=pendulum.datetime(2025, 7, 10, tz="Asia/Bangkok"), 
    catchup=False,
    tags=["api", "spectator_earth", "sentinel1", "etl", "postgresql", "minio", "geospatial"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    delete_task = PythonOperator(
        task_id="delete_old_sentinel1_data",
        python_callable=_delete_old_data,
        provide_context=True,
    )

    extract_transform_task = PythonOperator(
        task_id="extract_and_transform_sentinel1_data",
        python_callable=_extract_and_transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_sentinel1_data_to_postgres",
        python_callable=_load_data_to_postgres,
        provide_context=True,
    )

    delete_task >> extract_transform_task >> load_task