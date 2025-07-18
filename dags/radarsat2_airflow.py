from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import os
import tempfile
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values
from minio import Minio
import logging
import pandas as pd # Import pandas for potential Timestamp handling
from datetime import datetime, timedelta

# Configure logging
log = logging.getLogger(__name__)

# ====== CONFIGURATION ======
MINIO_URL = "172.27.127.90:9000"
ACCESS_KEY = "08RJAwKtz8QM8hRlVLY2"
SECRET_KEY = "kunOgQyMLnM6tqq7pxUY8jDMWuvKuuW04BA7Nt0j"
BUCKET_NAME = "gi-disaster"
S3_PREFIX = "SATPLAN/RAD2/"

# PostgreSQL Connection Details (using direct variables as requested)
PG_DBNAME = "aq_plan"
PG_USER = "gi.joke"
PG_PASSWORD = "Tawatcha1@2021"
PG_HOST = "172.27.154.25"
PG_PORT = "5432"

TABLE_NAME = "acquisition.radarsat2"

def _get_processed_files_from_db():
    conn = None
    cur = None
    processed_files = set()
    try:
        conn = psycopg2.connect(
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT begin_time FROM {TABLE_NAME}")
        processed_files = {row[0] for row in cur.fetchall()}
        log.info(f"Retrieved {len(processed_files)} previously processed files from DB.")
    except psycopg2.Error as e:
        log.error(f"❌ Error connecting to PostgreSQL or querying processed files: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    return processed_files

def _extract_shapefiles_from_minio(**kwargs):
    log.info("Starting extraction of shapefiles from MinIO...")
    processed_files = _get_processed_files_from_db()

    client = Minio(
        MINIO_URL,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

    objects = client.list_objects(BUCKET_NAME, prefix=S3_PREFIX, recursive=True)

    shapefile_sets = {}
    for obj in objects:
        if obj.object_name.endswith((".shp", ".dbf", ".shx", ".prj")):
            base = os.path.splitext(os.path.basename(obj.object_name))[0]
            shapefile_sets.setdefault(base, []).append(obj.object_name)

    serializable_dfs_for_xcom = []

    for base_name, file_keys in shapefile_sets.items():
        if base_name in processed_files:
            log.info(f"Skipping {base_name}: already processed.")
            continue

        required_exts = {".shp", ".dbf", ".shx"}
        found_exts = {os.path.splitext(f)[1] for f in file_keys}
        if not required_exts.issubset(found_exts):
            log.warning(f"⚠️ Missing required files ({required_exts - found_exts}) for {base_name}, skipping.")
            continue

        with tempfile.TemporaryDirectory() as tmpdir:
            local_paths = {}
            for file_key in file_keys:
                local_path = os.path.join(tmpdir, os.path.basename(file_key))
                try:
                    client.fget_object(BUCKET_NAME, file_key, local_path)
                    ext = os.path.splitext(file_key)[1]
                    local_paths[ext] = local_path
                except Exception as e:
                    log.error(f"Error downloading {file_key}: {e}. Skipping this shapefile set.")
                    local_paths = {}
                    break

            shp_path = local_paths.get(".shp")
            if not shp_path:
                log.error(f"No .shp file found or download failed for {base_name}. Skipping.")
                continue

            try:
                gdf = gpd.read_file(shp_path)
                
                # --- START OF DUPLICATE COLUMN HANDLING (Ignore duplicates) ---
                # This will keep the first occurrence of any duplicate column name and drop the rest.
                # It is crucial to do this right after reading the file.
                original_columns = gdf.columns.tolist()
                gdf = gdf.loc[:, ~gdf.columns.duplicated()]
                
                # Log if any columns were dropped due to duplication
                final_columns = gdf.columns.tolist()
                dropped_columns = [col for col in original_columns if original_columns.count(col) > 1 and col not in final_columns]
                if dropped_columns:
                    log.warning(f"Dropped duplicate columns for {base_name}: {list(set(dropped_columns))}. Kept first occurrences.")
                
                # Also, if 'satellite' column exists from the shapefile, rename it to 'satellite_name'
                # to align with your desired schema.
                if 'satellite' in gdf.columns:
                    gdf = gdf.rename(columns={'satellite': 'satellite_name'})
                    log.info(f"Renamed 'satellite' column to 'satellite_name' for {base_name}.")
                # --- END OF DUPLICATE COLUMN HANDLING ---

                if 'geometry' in gdf.columns:
                    if gdf.crs is None:
                        gdf = gdf.set_crs('EPSG:4326', allow_override=True)
                    elif gdf.crs != 'EPSG:4326':
                        gdf = gdf.to_crs('EPSG:4326')

                    gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt if x is not None else None)
                
                serializable_df = pd.DataFrame(gdf)
                serializable_df['base_name'] = base_name 
                serializable_dfs_for_xcom.append(serializable_df)
                log.info(f"Successfully extracted and serialized {base_name}.")

            except Exception as e:
                log.error(f"❌ Failed to read or serialize shapefile {base_name} at {shp_path}: {e}. Skipping.")
                continue

    kwargs['ti'].xcom_push(key='extracted_serializable_dfs', value=serializable_dfs_for_xcom)
    log.info(f"Finished extraction. Prepared {len(serializable_dfs_for_xcom)} shapefile sets for transformation.")


def _transform_shapefile_data(**kwargs):
    log.info("Starting transformation of shapefile data...")
    extracted_dfs = kwargs['ti'].xcom_pull(key='extracted_serializable_dfs', task_ids='extract_shapefiles_from_minio')

    if not extracted_dfs:
        log.warning("No DataFrames to transform.")
        kwargs['ti'].xcom_push(key='transformed_records', value=[])
        return

    all_transformed_records = []

    for i, df in enumerate(extracted_dfs):
        base_name = df['base_name'].iloc[0]
        log.info(f"Transforming DataFrame {i+1}/{len(extracted_dfs)}: {base_name}. Columns: {list(df.columns)}")

        gdf = None
        if 'geometry' in df.columns:
            try:
                geometry_series = gpd.GeoSeries.from_wkt(df['geometry'].astype(str), crs='EPSG:4326')
                gdf = gpd.GeoDataFrame(df.drop(columns=['geometry', 'base_name']), geometry=geometry_series, crs='EPSG:4326')
            except Exception as e:
                log.error(f"Error converting WKT to geometry in DataFrame {i} ({base_name}): {e}. Skipping this DataFrame.")
                continue
        else:
            log.warning(f"No 'geometry' column found in DataFrame {i} ({base_name}). Treating as regular DataFrame.")
            gdf = df.drop(columns=['base_name'])

        # Add/modify columns
        # Note: 'satellite_name' should now be consistent due to handling in _extract_shapefiles_from_minio
        gdf['gid'] = 1
        gdf['path_number'] = None
        gdf['begin_time'] = base_name
        
        # Ensure 'satellite_name' is set, either from existing or new
        if 'satellite_name' not in gdf.columns:
            gdf['satellite_name'] = "Radarsat-2"
        # If 'satellite_name' already exists and is from the original file, it should be kept.
        # Otherwise, if it was 'satellite' and got renamed, it's fine.
        # If the value from the file is not suitable, you might explicitly overwrite it:
        # gdf['satellite_name'] = "Radarsat-2" # Uncomment this if you always want to force this value

        records_for_db = []
        for index, row in gdf.iterrows():
            geom_wkt = row['geometry'].wkt if 'geometry' in row and row['geometry'] is not None else None
            
            records_for_db.append((
                row.get('gid', None),
                row.get('satellite_name', None), # This should now exist due to prior handling
                row.get('path_number', None),
                row.get('begin_time', None),
                geom_wkt
            ))
        all_transformed_records.extend(records_for_db)
        log.info(f"✅ Transformed {len(records_for_db)} records from {base_name}.")
        
    kwargs['ti'].xcom_push(key='transformed_records', value=all_transformed_records)
    log.info(f"Finished transformation. Total {len(all_transformed_records)} records prepared for loading.")


def _load_data_to_postgres(**kwargs):
    log.info("Starting data loading to PostgreSQL...")
    transformed_records = kwargs['ti'].xcom_pull(key='transformed_records', task_ids='transform_shapefile_data')

    if not transformed_records:
        log.warning("No data to load into PostgreSQL.")
        return

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=PG_DBNAME,
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT
        )
        cur = conn.cursor()

        insert_query = f"""
            INSERT INTO {TABLE_NAME} (gid, satellite_name, path_number, begin_time, geom)
            VALUES %s
        """
        log.info(f"Inserting {len(transformed_records)} records into {TABLE_NAME}...")
        execute_values(cur, insert_query, transformed_records, template="""(
            %s, %s, %s, %s, ST_GeomFromText(%s, 4326)
        )""")
        conn.commit()
        log.info(f"Successfully loaded {len(transformed_records)} records into {TABLE_NAME}.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        log.error(f"❌ Error during PostgreSQL load: {e}")
        raise
    except Exception as e:
        log.error(f"An unexpected error occurred during load: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


with DAG(
    dag_id="import-radarsat2-from-minio-taskflow",
    schedule="*/5 * * * *", # ทำงานทุก 5 นาที
    start_date=pendulum.datetime(2025, 7, 4, tz="Asia/Bangkok"), # ตั้ง start_date เป็นวันที่ 4 กรกฎาคม 2568 เวลา 00:00 น. (ตามเวลาไทย)
    catchup=False,
    tags=["minio", "shapefile", "postgresql", "radarsat2", "etl"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    extract_task = PythonOperator(
        task_id="extract_shapefiles_from_minio",
        python_callable=_extract_shapefiles_from_minio,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_shapefile_data",
        python_callable=_transform_shapefile_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task