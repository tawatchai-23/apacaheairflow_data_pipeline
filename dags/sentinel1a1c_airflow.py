from __future__ import annotations
import os
import pendulum
import requests
import psycopg2
from datetime import datetime, timedelta
import json
import pytz
from shapely.geometry import shape, box
from psycopg2.extras import Json 
from minio import Minio

from psycopg2 import sql 
from psycopg2.extras import execute_values 
from psycopg2.extensions import AsIs 

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin 


log = LoggingMixin().log

# ====== CONFIGURATION ======
# Spectator Earth API
SPECTATOR_API_KEY = "SPECTATOR_API_KEY"
SPECTATOR_API_URL = "SPECTATOR_API_URL"

# PostgreSQL Connection Details (using direct variables as requested)
PG_DBNAME = "*******"
PG_USER = "*******"
PG_PASSWORD = "*******"
PG_HOST = "*******"
PG_PORT = "*******"

# Target table and satellite names
TABLE_NAME = "*******"
SATELLITES_TO_FETCH = "*******" 
SATELLITES_FOR_DELETION = "*******" 

# MinIO Staging Configuration
MINIO_URL = "*******" # Use the same MinIO URL as your previous DAG
MINIO_ACCESS_KEY = "*******" # Your access key
MINIO_SECRET_KEY = "*******" # Your secret key
STAGING_BUCKET_NAME = "*******" # A new bucket specifically for staging data
STAGING_PREFIX = "*******"

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
    delete_from_date = kwargs['data_interval_start'].in_timezone(THAILAND_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    
    conn = None
    cursor = None
    try:
        conn = _get_pg_connection()
        cursor = conn.cursor()
        
        in_clause = sql.SQL("IN ({})").format(
            sql.SQL(', ').join(map(sql.Literal, SATELLITES_FOR_DELETION))
        )

        delete_query = sql.SQL('''
            DELETE FROM {}
            WHERE begin_time >= %s AND satellite_name {};
        ''').format(
            sql.Identifier(TABLE_NAME.split('.')[0], TABLE_NAME.split('.')[1]),
            in_clause
        )
        
        log.info(f"Executing delete query: {cursor.mogrify(delete_query, (delete_from_date,)).decode('utf-8')}")

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
    start_date_for_fetch = kwargs['data_interval_start'].in_timezone(THAILAND_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    
    all_filtered_records = []
    coordinates_set = set()

    for i in range(11): # วนลูป 11 ครั้ง (i = 0 ถึง 10)
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

        columns = [
            'type', 'coordinates', 'swath', 'datatake_id', 'polarisation',
            'orbit_absolute', 'orbit_relative', 'begin_time', 'begin_time_nottimezone',
            'end_time', 'end_time_nottimezone', 'satellite', 'satellite_name','geom' # geom เป็นคอลัมน์ PostGIS
        ]

        insert_query = sql.SQL('''
            INSERT INTO {} ({})
            VALUES %s
        ''').format(
            sql.Identifier(TABLE_NAME.split('.')[0], TABLE_NAME.split('.')[1]),
            sql.SQL(', ').join(map(sql.Identifier, columns))
        )
    
        data_to_insert = []
        for record in transformed_records:
            geojson_for_geom = record.get('coordinates')
            
            geom_sql_expression = f"ST_SetSRID(ST_GeomFromGeoJSON('{geojson_for_geom.replace("'", "''")}'), 4326)"
            
            geom_as_is = AsIs(geom_sql_expression)

            data_to_insert.append((
                record.get('type'),
                record.get('coordinates'),
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
                record.get('satellite'), 
                geom_as_is 
            ))

        log.info(f"กำลังแทรก {len(data_to_insert)} รายการเข้าสู่ {TABLE_NAME}...")
        
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

with DAG(
    dag_id="import-sentinel1A-1C-from-api-taskflow",
    schedule="0 7 * * *", 
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