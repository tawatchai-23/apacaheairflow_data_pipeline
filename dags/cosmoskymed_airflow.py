from __future__ import annotations

import os
import tempfile
import pendulum
import psycopg2 # Import psycopg2 directly

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable # Still useful for TABLE_NAME if you want to keep it dynamic

import geopandas as gpd
from minio import Minio
from psycopg2.extras import execute_values

# Define the DAG's default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
}

# ====== HARDCODED CONFIGURATIONS ======
# WARNING: Hardcoding credentials is NOT recommended for production environments.
# It poses significant security risks and makes credential management difficult.

# MinIO Config
MINIO_URL = "172.27.127.90:9000"
MINIO_ACCESS_KEY = "08RJAwKtz8QM8hRlVLY2"
MINIO_SECRET_KEY = "kunOgQyMLnM6tqq7pxUY8jDMWuvKuuW04BA7Nt0j"
MINIO_BUCKET_NAME = "gi-disaster"
MINIO_S3_PREFIX = "SATPLAN/CSK/"

# PostgreSQL Config
PG_DBNAME = "aq_plan"
PG_USER = "gi.joke"
PG_PASSWORD = "Tawatcha1@2021"
PG_HOST = "172.27.154.25"
PG_PORT = "5432"
PG_TABLE_NAME = "acquisition.cosmoskymed" # Hardcoded table name

# If you still want to keep TABLE_NAME dynamic via Airflow Variable, uncomment this and remove the hardcoded PG_TABLE_NAME above
# PG_TABLE_NAME = Variable.get("POSTGRES_TABLE_NAME") # Requires an Airflow Variable named POSTGRES_TABLE_NAME


# ====== ETL FUNCTIONS ======

def _extract_shapefiles_from_minio(**kwargs):
    """
    Extract stage: Connects to MinIO and lists all relevant shapefile components.
    Pushes a list of shapefile_sets (base_name, file_keys) to XCom.
    """
    ti = kwargs["ti"]
    print("🚀 Starting Extract phase: Connecting to MinIO...")

    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    objects = client.list_objects(MINIO_BUCKET_NAME, prefix=MINIO_S3_PREFIX, recursive=True)

    shapefile_sets = {}
    for obj in objects:
        if obj.object_name.lower().endswith((".shp", ".dbf", ".shx", ".prj")):
            basename = os.path.splitext(os.path.basename(obj.object_name))[0]
            shapefile_sets.setdefault(basename, []).append(obj.object_name)

    # Filter out incomplete sets early
    valid_shapefile_sets = []
    required_exts = [".shp", ".dbf", ".shx"]
    for base_name, files in shapefile_sets.items():
        if all(any(f.lower().endswith(ext) for f in files) for ext in required_exts):
            valid_shapefile_sets.append({"base_name": base_name, "files": files})
        else:
            print(f"⚠️ Missing required files for {base_name}, will skip during transform.")

    # Push the list of valid shapefile sets to XCom for the next task
    ti.xcom_push(key="shapefile_sets", value=valid_shapefile_sets)
    print(f"✅ Extract phase complete. Found {len(valid_shapefile_sets)} valid shapefile sets.")


def _transform_and_prepare_data(**kwargs):
    """
    Transform stage: Downloads shapefiles, reads them into GeoPandas,
    adds/modifies columns, and prepares data for bulk insert.
    Pushes the transformed data as a list of tuples to XCom.
    """
    ti = kwargs["ti"]
    print("🚧 Starting Transform phase: Processing shapefiles...")

    # Pull shapefile sets from XCom
    shapefile_sets = ti.xcom_pull(key="shapefile_sets", task_ids="extract_shapefiles")

    if not shapefile_sets:
        print("No shapefile sets to transform. Exiting transform task.")
        ti.xcom_push(key="transformed_data", value=[]) # Push empty list
        return

    client = Minio(
        MINIO_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    all_transformed_data = []

    for item in shapefile_sets:
        base_name = item["base_name"]
        files = item["files"]

        with tempfile.TemporaryDirectory() as tmpdir:
            local_paths = {}
            for file_key in files:
                local_filename = os.path.join(tmpdir, os.path.basename(file_key))
                client.fget_object(MINIO_BUCKET_NAME, file_key, local_filename)
                local_paths[os.path.splitext(file_key)[1].lower()] = local_filename

            shp_path = local_paths.get(".shp")
            if not shp_path:
                print(f"❌ .shp file not found for {base_name}, skipping transform.")
                continue

            try:
                gdf = gpd.read_file(shp_path)
                print(f"📖 Successfully read shapefile: {base_name}")

                # Transform: Add/Modify columns
                gdf['satellite_name'] = 'Cosmoskymed'
                gdf['begin_time'] = base_name
                gdf['path_number'] = None # Example: if path_number needs to be derived

                selected_columns = ['satellite_name', 'begin_time', 'geometry', 'path_number']
                gdf = gdf[selected_columns]

                # Prepare data for bulk insert
                data_for_insert = [
                    (
                        row['satellite_name'],
                        row['begin_time'],
                        row['path_number'],
                        row['geometry'].wkt # Convert GeoPandas geometry to WKT
                    ) for _, row in gdf.iterrows()
                ]
                all_transformed_data.extend(data_for_insert)
                print(f"✨ Transformed {len(data_for_insert)} records from {base_name}")

            except Exception as e:
                print(f"❌ Failed to read or transform shapefile {base_name}: {e}")
                continue
    
    ti.xcom_push(key="transformed_data", value=all_transformed_data)
    print(f"✅ Transform phase complete. Total records ready for load: {len(all_transformed_data)}")


def _load_data_to_postgres(**kwargs):
    """
    Load stage: Connects to PostgreSQL directly and bulk inserts the transformed data.
    """
    ti = kwargs["ti"]
    print("⬇️ Starting Load phase: Inserting data into PostgreSQL...")

    # Pull transformed data from XCom
    transformed_data = ti.xcom_pull(key="transformed_data", task_ids="transform_data")

    if not transformed_data:
        print("No data to load. Exiting load task.")
        return

    conn = None
    cur = None
    try:
        # Direct PostgreSQL connection using hardcoded credentials
        conn = psycopg2.connect(
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        print("Connected to PostgreSQL directly.")

        # Clear existing data before inserting (if this is the desired behavior)
        cur.execute(f"DELETE FROM {PG_TABLE_NAME}")
        print(f"🗑️ Cleared existing data from {PG_TABLE_NAME}.")

        insert_query = f"""
            INSERT INTO {PG_TABLE_NAME} (satellite_name, begin_time, path_number, geom)
            VALUES %s
        """
        execute_values(cur, insert_query, transformed_data)
        conn.commit()
        print(f"✅ Load phase complete. Successfully inserted {len(transformed_data)} records into {PG_TABLE_NAME}.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        print(f"❌ PostgreSQL connection or query error: {e}")
        raise # Re-raise the exception to mark the task as failed
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ An unexpected error occurred during Load phase: {e}")
        raise # Re-raise the exception to mark the task as failed
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


with DAG(
    dag_id="import-cosmoskymed-from-minio-taskflow",
    default_args=default_args,
    description="ETL pipeline for Cosmoskymed shapefiles from MinIO to PostgreSQL with ALL hardcoded credentials",
    schedule="*/5 * * * *", # ทำงานทุก 5 นาที
    start_date=pendulum.datetime(2025, 7, 4, tz="Asia/Bangkok"), # ตั้ง start_date เป็นวันที่ 4 กรกฎาคม 2568 เวลา 00:00 น. (ตามเวลาไทย)
    catchup=False,
    tags=["etl", "minio", "postgresql", "geospatial", "hardcoded"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_shapefiles",
        python_callable=_extract_shapefiles_from_minio,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_and_prepare_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
        provide_context=True,
    )

    # Define task dependencies (ETL flow)
    extract_task >> transform_task >> load_task