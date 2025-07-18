from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import io
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from minio import Minio
import pytz
from datetime import datetime, timedelta
import logging
import base64

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
# log: logging.log = logging.getLogger("airflow")
# log.setLevel(logging.INFO)
# log.info("Yes, you will see this log :)")

# ====== CONFIG ======
MINIO_URL = "172.27.127.90:9000"
ACCESS_KEY = "08RJAwKtz8QM8hRlVLY2"
SECRET_KEY = "kunOgQyMLnM6tqq7pxUY8jDMWuvKuuW04BA7Nt0j"
BUCKET_NAME = "gi-disaster"
S3_PREFIX = "SATPLAN/THEOS-2/"
TABLE_NAME = "acquisition.theos2"

# Timezone
bangkok_tz = pytz.timezone("Asia/Bangkok")

# ====== FUNCTIONS (Encapsulated for Airflow Tasks) ======

def _connect_to_postgres():
    """Establishes and returns a PostgreSQL connection and cursor."""
    try:
        conn = psycopg2.connect(
            dbname="aq_plan",
            user="gi.joke",
            password="Tawatcha1@2021",
            host="172.27.154.25",
            port="5432"
        )
        return conn, conn.cursor()
    except psycopg2.Error as e:
        logger.error(f"❌ Error connecting to the database: {e}")
        raise

# def _connect_to_minio():
#     """Establishes and returns a MinIO client."""
#     try:
#         client = Minio(
#             MINIO_URL,
#             access_key=ACCESS_KEY,
#             secret_key=SECRET_KEY,
#             secure=False
#         )
#         return client
#     except Exception as e:
#         logger.error(f"❌ Error connecting to MinIO: {e}")
#         raise

def convert_utc_to_bangkok(series):
    """Converts a pandas Series of UTC timestamps to Bangkok time, removing timezone info."""
    return pd.to_datetime(series, errors='coerce') \
             .dt.tz_localize('UTC') \
             .dt.tz_convert(bangkok_tz) \
             .dt.tz_localize(None)

def extract_files_from_s3(**kwargs):
    """
    Task to extract GeoJSON files from MinIO S3.
    Pushes a list of file contents (as bytes) to XCom.
    """
    client = Minio(MINIO_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
    
    geojson_files_data = []
    objects = client.list_objects(BUCKET_NAME, prefix=S3_PREFIX, recursive=True)

    for obj in objects:
        if not obj.object_name.endswith(".geojson"):
            continue

        filename = obj.object_name.split("/")[-1]
        logger.info(f"📄 Extracting: {filename}")

        try:
            response = client.get_object(BUCKET_NAME, obj.object_name)
            file_data = response.read()

            # เข้ารหัส bytes เป็น Base64 string ก่อนส่งผ่าน XCom
            encoded_file_data = base64.b64encode(file_data).decode('utf-8') 

            geojson_files_data.append({"filename": filename, "data": encoded_file_data})
        except Exception as e:
            logger.error(f"❌ Failed to read GeoJSON {filename} from MinIO: {e}")
            # Decide whether to continue or raise an exception based on your error handling strategy
            continue
    
    if not geojson_files_data:
        logger.warning("No GeoJSON files found in the specified S3 prefix.")
        # You might want to fail the task if no files are found, or allow it to proceed with empty data.
        # For now, we'll push an empty list.
        # raise ValueError("No GeoJSON files found to process.") 
    
    kwargs['ti'].xcom_push(key='geojson_files_data', value=geojson_files_data)
    logger.info(f"✅ Extracted {len(geojson_files_data)} GeoJSON files.")


def transform_shapefile(**kwargs):
    """
    Task to transform GeoJSON data.
    Pulls file data from XCom, processes it, and pushes transformed DataFrames to XCom.
    """
    ti = kwargs['ti']
    geojson_files_data = ti.xcom_pull(key='geojson_files_data', task_ids='extract_files_from_s3')

    if not geojson_files_data:
        logger.info("No GeoJSON files to transform. Skipping transformation.")
        ti.xcom_push(key='transformed_dataframes', value=[])
        return

    transformed_dataframes = []
    for file_info in geojson_files_data:
        filename = file_info["filename"]
        encoded_file_data = file_info["data"] # ตอนนี้เป็น Base64 string

        logger.info(f"🔄 Transforming: {filename}")
        try:
            file_data_bytes = base64.b64decode(encoded_file_data)
            file_like = io.BytesIO(file_data_bytes)
            # file_like = io.BytesIO(file_data)
            gdf = gpd.read_file(file_like)
        except Exception as e:
            logger.error(f"❌ Failed to read GeoJSON {filename} for transformation: {e}")
            continue

        if gdf.crs is None or gdf.crs != "EPSG:4326":
            gdf = gdf.set_crs("EPSG:4326", allow_override=True)

        # Add default values
        gdf['satellite_name'] = "THEOS-2"
        gdf['path_number'] = None

        # Convert times to Bangkok timezone
        gdf['acq_start'] = convert_utc_to_bangkok(gdf['acqStart'])
        gdf['acq_end'] = convert_utc_to_bangkok(gdf['acqEnd'])
        gdf['start_date'] = convert_utc_to_bangkok(gdf['startDate'])
        gdf['end_date'] = convert_utc_to_bangkok(gdf['endDate'])

        # Generate begin_time
        gdf['begin_time'] = gdf['acq_start'].apply(
            lambda x: f"T2V_{x.strftime('%Y%m%d')}_0000" if pd.notnull(x) else None
        )

        # Rename columns
        gdf.rename(columns={
            'id': 'id',
            'acqID': 'acq_id',
            'state': 'state',
            'Satellite': 'satellite',
            'Mesh': 'mesh'
        }, inplace=True)

        # Select desired columns
        columns = [
            'id', 'acq_id', 'acq_start', 'acq_end', 'state',
            'start_date', 'end_date', 'satellite', 'satellite_name',
            'mesh', 'path_number', 'begin_time', 'geometry'
        ]
        
        # Ensure all required columns exist after renaming/creation
        missing_columns = [col for col in columns if col not in gdf.columns]
        if missing_columns:
            logger.warning(f"Missing expected columns in {filename}: {missing_columns}. Skipping this file.")
            continue

        gdf = gdf[columns]

        # รายชื่อคอลัมน์ที่เป็น Timestamp ที่คุณต้องการแปลงเป็น String
        timestamp_columns = ['acq_start', 'acq_end', 'start_date', 'end_date']

        for col in timestamp_columns:
            if col in gdf.columns: # ตรวจสอบว่าคอลัมน์มีอยู่จริง
                # ใช้ dt.isoformat() เพื่อแปลง Timestamp เป็น String ในรูปแบบ ISO 8601
                # .fillna(pd.NA) เพื่อจัดการค่าว่าง และ .astype(str) เพื่อให้แน่ใจว่าเป็น String
                gdf[col] = gdf[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)

                # Note:  # จากนั้นแปลงเป็น str เพื่อให้แน่ใจว่าเป็น String ที่สามารถ Serialize ได้
                # และเติมค่าว่างด้วย '' แทน None เพื่อหลีกเลี่ยงปัญหาใน JSON/PostgreSQL
                gdf[col] = gdf[col].astype(str).replace('None', '') 

        transformed_dataframes.append(gdf.to_json()) # Convert to JSON for XCom storage

    ti.xcom_push(key='transformed_dataframes', value=transformed_dataframes)
    logger.info(f"✅ Transformed {len(transformed_dataframes)} GeoJSON files.")


def load_data_to_pgadmin(**kwargs):
    """
    Task to load transformed data into PostgreSQL.
    Pulls transformed DataFrames from XCom and inserts them into the database.
    """
    ti = kwargs['ti']
    transformed_dataframes_json = ti.xcom_pull(key='transformed_dataframes', task_ids='transform_shapefile')

    if not transformed_dataframes_json:
        logger.info("No transformed dataframes to load. Skipping loading.")
        return

    conn, cur = _connect_to_postgres()
    
    total_inserted_rows = 0
    try:
        for gdf_json in transformed_dataframes_json:
            gdf = gpd.read_file(io.StringIO(gdf_json)) # Read back from JSON

            records = []
            for _, row in gdf.iterrows():
                records.append((
                    row['id'], row['acq_id'], row['acq_start'], row['acq_end'], row['state'],
                    row['start_date'], row['end_date'], row['satellite'], row['satellite_name'],
                    row['mesh'], row['path_number'], row['begin_time'],
                    row['geometry'].wkt if row['geometry'] else None
                ))

            if not records:
                logger.info(f"No records to insert for a transformed GeoJSON file.")
                continue

            insert_sql = f"""
                INSERT INTO {TABLE_NAME} (
                    id, acq_id, acq_start, acq_end, state,
                    start_date, end_date, satellite, satellite_name,
                    mesh, path_number, begin_time, geom
                ) VALUES %s
                ON CONFLICT (id) DO NOTHING
            """

            try:
                execute_values(cur, insert_sql, records, template="""
                    (%s, %s, %s, %s, %s,
                     %s, %s, %s, %s,
                     %s, %s, %s, ST_GeomFromText(%s, 4326))
                """)
                conn.commit()
                logger.info(f"✅ Inserted {len(records)} rows into {TABLE_NAME}.")
                total_inserted_rows += len(records)
            except psycopg2.Error as e:
                conn.rollback()
                logger.error(f"❌ Error inserting records: {e}")
                # Depending on your error handling, you might want to raise the exception
                # or just log and continue to the next file.
                continue
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            logger.info("✅ PostgreSQL connection closed.")
    
    logger.info(f"🎉 Successfully loaded a total of {total_inserted_rows} rows into PostgreSQL.")


with DAG(
    dag_id="import_theos2_from_minio_taskflow",
    schedule="*/5 * * * *", # ทำงานทุก 5 นาที
    start_date=pendulum.datetime(2025, 7, 4, tz="Asia/Bangkok"), # ตั้ง start_date เป็นวันที่ 4 กรกฎาคม 2568 เวลา 00:00 น. (ตามเวลาไทย)
    catchup=False,
    tags=["geo", "minio", "postgresql", "etl"],
    doc_md="""
    ### GeoJSON to PostgreSQL Ingestion DAG

    This DAG extracts GeoJSON files from MinIO S3, transforms the data to match
    the `acquisition.theos2` table schema in PostgreSQL, and then loads the data.
    It handles timezone conversion and uses `ON CONFLICT (id) DO NOTHING` for idempotency.
    """
) as dag:
    
    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract_files_from_s3",
        python_callable=extract_files_from_s3,
        provide_context=True, # Required to access ti (TaskInstance) for XCom
    )

    transform_task = PythonOperator(
        task_id="transform_shapefile",
        python_callable=transform_shapefile,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_data_to_pgadmin",
        python_callable=load_data_to_pgadmin,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> extract_task >> transform_task >> load_task >> end