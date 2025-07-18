# import os
# import geopandas as gpd
# import psycopg2
# from psycopg2.extras import execute_values
# import pandas as pd
# from datetime import timedelta
# from minio import Minio
# from io import BytesIO

# # ====== 🧠 CONFIGURATION ======
# MINIO_URL = "172.27.127.90:9000"
# ACCESS_KEY = "08RJAwKtz8QM8hRlVLY2"
# SECRET_KEY = "kunOgQyMLnM6tqq7pxUY8jDMWuvKuuW04BA7Nt0j"
# BUCKET_NAME = "gi-disaster"
# S3_PREFIX = "SATPLAN/Thaichote/"

# TABLE_NAME = "acquisition.thaichote_old"

# # ====== 🐘 CONNECT TO POSTGRESQL ======
# try:
#     conn = psycopg2.connect(
#         dbname="aq_plan",
#         user="gi.joke",
#         password="Tawatcha1@2021",
#         host="172.27.154.25",
#         port="5432"
#     )
#     cur = conn.cursor()
# except psycopg2.Error as e:
#     print(f"❌ Error connecting to the database: {e}")
#     exit()

# # ====== 📡 CONNECT TO MINIO ======
# client = Minio(
#     MINIO_URL,
#     access_key=ACCESS_KEY,
#     secret_key=SECRET_KEY,
#     secure=False
# )

# def import_new_geojson():
#     try:
#         # Delete all existing data from the table
#         cur.execute(f"DELETE FROM {TABLE_NAME} where name IS NOT NULL")

#         # Get list of objects from MinIO bucket with prefix
#         objects = client.list_objects(BUCKET_NAME, prefix=S3_PREFIX, recursive=True)

#         for obj in objects:
#             if obj.object_name.endswith(".geojson"):
#                 try:
#                     # Get object data from MinIO
#                     response = client.get_object(BUCKET_NAME, obj.object_name)
#                     geojson_data = response.read()
                    
#                     # Read GeoJSON from bytes
#                     gdf = gpd.read_file(BytesIO(geojson_data))
#                 except Exception as e:
#                     print(f"Error reading GeoJSON {obj.object_name}: {e}")
#                     continue
#                 finally:
#                     response.close()
#                     response.release_conn()

#                 # Debug: Print available columns in GeoJSON
#                 print(f"Columns in {obj.object_name}: {list(gdf.columns)}")

#                 # Add or map columns to match the table structure
#                 gdf['satellite_name'] = 'Thaichote'
                
#                 # Process StartAcqDate
#                 if 'StartAcqDate' in gdf.columns:
#                     try:
#                         gdf['datetime_temp'] = pd.to_datetime(
#                             gdf['StartAcqDate'],
#                             format='%m/%d/%Y %I:%M:%S %p',
#                             errors='coerce'
#                         )
#                         # For start_acq_date (TIMESTAMP WITH TIME ZONE, UTC+7)
#                         gdf['start_acq_date'] = gdf['datetime_temp'] + timedelta(hours=7)
#                         # For begin_time (text, format TH1_YYYYMMDD_0000)
#                         gdf['begin_time'] = gdf['datetime_temp'].apply(
#                             lambda x: f"TH1_{x.strftime('%Y%m%d')}_0000" if pd.notnull(x) else None
#                         )
                        
#                         if gdf['start_acq_date'].isnull().any():
#                             print(f"Warning: Some start_acq_date values in {obj.object_name} are null")
#                         if gdf['begin_time'].isnull().any():
#                             print(f"Warning: Some begin_time values in {obj.object_name} are null")
                            
#                         gdf = gdf.drop(columns=['datetime_temp'])
#                     except Exception as e:
#                         print(f"Error processing StartAcqDate in {obj.object_name}: {e}")
#                         gdf['start_acq_date'] = None
#                         gdf['begin_time'] = None
#                 else:
#                     print(f"StartAcqDate not found in {obj.object_name}")
#                     gdf['start_acq_date'] = None
#                     gdf['begin_time'] = None

#                 # Debug: Print sample of times
#                 print(f"Sample start_acq_date: {gdf['start_acq_date'].head().to_list()}")
#                 print(f"Sample begin_time: {gdf['begin_time'].head().to_list()}")

#                 # Ensure geometry is in SRID 4326
#                 if gdf.crs is None or gdf.crs != 'EPSG:4326':
#                     gdf = gdf.set_crs('EPSG:4326', allow_override=True)

#                 # Select and rename columns to match table
#                 column_mapping = {
#                     'name': 'name',
#                     'satellite_name': 'satellite_name',
#                     'ReqName': 'req_name',
#                     'StripName': 'strip_name',
#                     'RevNumber': 'rev_number',
#                     'start_acq_date': 'start_acq_date',
#                     'Group': 'group_path',
#                     'ReferenceMiseo': 'reference_miseo',
#                     'begin_time': 'begin_time',
#                     'geometry': 'geometry'
#                 }
                
#                 available_columns = [k for k in column_mapping.keys() if k in gdf.columns]
#                 gdf = gdf[available_columns]
                
#                 # Convert to match table column names
#                 gdf = gdf.rename(columns={k: v for k, v in column_mapping.items() if k in gdf.columns})

#                 # Convert GeoDataFrame to list of tuples for insertion
#                 data = []
#                 for _, row in gdf.iterrows():
#                     geom_wkt = row['geometry'].wkt if row['geometry'] is not None else None
#                     row_data = (
#                         row.get('name', None),
#                         row.get('satellite_name', 'Thaichote'),
#                         row.get('req_name', None),
#                         row.get('strip_name', None),
#                         row.get('rev_number', None),
#                         row.get('start_acq_date', None),
#                         row.get('group_path', None),
#                         row.get('reference_miseo', None),
#                         row.get('begin_time', None),
#                         geom_wkt
#                     )
#                     data.append(row_data)

#                 # Insert data into the table
#                 insert_query = f"""
#                     INSERT INTO {TABLE_NAME} (
#                         name, satellite_name, req_name, strip_name, rev_number,
#                         start_acq_date, group_path, reference_miseo, begin_time, geom
#                     )
#                     VALUES %s
#                     """
#                 try:
#                     execute_values(cur, insert_query, data, template="""(
#                         %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                         ST_GeomFromText(%s, 4326)
#                     )""")
#                     print(f"Successfully imported {obj.object_name}")
#                 except psycopg2.Error as e:
#                     print(f"Error inserting data for {obj.object_name}: {e}")
#                     conn.rollback()
#                     continue

#     except Exception as e:
#         print(f"An error occurred: {e}")
#         conn.rollback()
#     else:
#         conn.commit()

# # Run the function
# import_new_geojson()

# # Close the connection
# cur.close()
# conn.close()

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import os
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from datetime import timedelta
from minio import Minio
from io import BytesIO
import logging

# Configure logging
log = logging.getLogger(__name__)

# ====== 🧠 CONFIGURATION ======
MINIO_URL = "172.27.127.90:9000"
ACCESS_KEY = "08RJAwKtz8QM8hRlVLY2"
SECRET_KEY = "kunOgQyMLnM6tqq7pxUY8jDMWuvKuuW04BA7Nt0j"
BUCKET_NAME = "gi-disaster"
S3_PREFIX = "SATPLAN/Thaichote/"

# PostgreSQL Connection Details (ใช้ตัวแปรตรงๆ แทน Hook)
PG_DBNAME = "aq_plan"
PG_USER = "gi.joke"
PG_PASSWORD = "Tawatcha1@2021"
PG_HOST = "172.27.154.25"
PG_PORT = "5432"

TABLE_NAME = "acquisition.thaichote"

def _extract_geojson_from_minio(**kwargs):
    """
    Extracts GeoJSON files from MinIO. Converts GeoDataFrame to a serializable
    format (Pandas DataFrame with geometry as WKT) before pushing to XCom.
    """
    log.info("Starting extraction from MinIO...")
    client = Minio(
        MINIO_URL,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=False
    )

    serializable_dfs = [] # Will store Pandas DataFrames
    objects = client.list_objects(BUCKET_NAME, prefix=S3_PREFIX, recursive=True)

    for obj in objects:
        if obj.object_name.endswith(".geojson"):
            log.info(f"Extracting object: {obj.object_name}")
            response = None
            try:
                response = client.get_object(BUCKET_NAME, obj.object_name)
                geojson_data = response.read()
                gdf = gpd.read_file(BytesIO(geojson_data))
                
                # Convert geometry to WKT string for serialization
                if 'geometry' in gdf.columns:
                    # Make sure CRS is set before converting to WKT to avoid issues
                    if gdf.crs is None:
                        gdf = gdf.set_crs('EPSG:4326', allow_override=True)
                    elif gdf.crs != 'EPSG:4326':
                        gdf = gdf.to_crs('EPSG:4326')

                    gdf['geometry'] = gdf['geometry'].apply(lambda x: x.wkt if x is not None else None)
                
                # Convert GeoDataFrame to Pandas DataFrame for XCom compatibility
                serializable_df = pd.DataFrame(gdf)
                serializable_dfs.append(serializable_df)

            except Exception as e:
                log.error(f"Error reading or processing GeoJSON {obj.object_name}: {e}")
                # Continue to the next file even if one fails
            finally:
                if response:
                    response.close()
                    response.release_conn()
    
    # Push the list of Pandas DataFrames to XCom
    kwargs['ti'].xcom_push(key='extracted_serializable_dfs', value=serializable_dfs)
    log.info(f"Finished extraction. Found {len(serializable_dfs)} GeoJSON files and pushed as Pandas DataFrames.")


def _transform_geojson_data(**kwargs):
    """
    Transforms the extracted GeoJSON data. Converts geometry back from WKT
    to shapely geometry objects for GeoPandas operations.
    Ensures all data pushed to XCom is serializable.
    """
    log.info("Starting transformation of GeoJSON data...")
    # Pull the list of Pandas DataFrames from XCom
    extracted_dfs = kwargs['ti'].xcom_pull(key='extracted_serializable_dfs', task_ids='extract_geojson_from_minio')

    if not extracted_dfs:
        log.warning("No DataFrames to transform.")
        kwargs['ti'].xcom_push(key='transformed_data', value=[])
        return

    transformed_records = []

    for i, df in enumerate(extracted_dfs):
        log.info(f"Transforming DataFrame {i+1}/{len(extracted_dfs)}. Columns: {list(df.columns)}")

        # Convert Pandas DataFrame back to GeoDataFrame for geospatial operations
        # Convert 'geometry' column (WKT) back to shapely geometry objects
        gdf = None
        if 'geometry' in df.columns:
            try:
                # Need to handle potential None values in geometry column during WKT conversion
                geometry_series = gpd.GeoSeries.from_wkt(df['geometry'].astype(str), crs='EPSG:4326')
                gdf = gpd.GeoDataFrame(df.drop(columns=['geometry']), geometry=geometry_series, crs='EPSG:4326')
            except Exception as e:
                log.error(f"Error converting WKT to geometry in DataFrame {i}: {e}. Skipping this DataFrame.")
                continue
        else:
            log.warning(f"No 'geometry' column found in DataFrame {i}. Treating as regular DataFrame.")
            gdf = df # Treat as a regular DataFrame if no geometry

        # Now continue with your existing transformation logic on gdf
        gdf['satellite_name'] = 'Thaichote'
        
        # Process StartAcqDate
        if 'StartAcqDate' in gdf.columns:
            try:
                gdf['datetime_temp'] = pd.to_datetime(
                    gdf['StartAcqDate'],
                    format='%m/%d/%Y %I:%M:%S %p',
                    errors='coerce'
                )
                gdf['start_acq_date'] = gdf['datetime_temp'] + timedelta(hours=7) # UTC+7
                gdf['begin_time'] = gdf['datetime_temp'].apply(
                    lambda x: f"TH1_{x.strftime('%Y%m%d')}_0000" if pd.notnull(x) else None
                )
                
                if gdf['start_acq_date'].isnull().any():
                    log.warning(f"Warning: Some start_acq_date values in GeoDataFrame {i} are null")
                if gdf['begin_time'].isnull().any():
                    log.warning(f"Warning: Some begin_time values in GeoDataFrame {i} are null")
                    
                gdf = gdf.drop(columns=['datetime_temp'])
            except Exception as e:
                log.error(f"Error processing StartAcqDate in GeoDataFrame {i}: {e}")
                gdf['start_acq_date'] = None
                gdf['begin_time'] = None
        else:
            log.warning(f"StartAcqDate not found in GeoDataFrame {i}")
            gdf['start_acq_date'] = None
            gdf['begin_time'] = None

        log.info(f"Sample start_acq_date: {gdf['start_acq_date'].head().to_list()}")
        log.info(f"Sample begin_time: {gdf['begin_time'].head().to_list()}")

        # Select and rename columns to match table
        column_mapping = {
            'name': 'name',
            'satellite_name': 'satellite_name',
            'ReqName': 'req_name',
            'StripName': 'strip_name',
            'RevNumber': 'rev_number',
            'start_acq_date': 'start_acq_date',
            'Group': 'group_path',
            'ReferenceMiseo': 'reference_miseo',
            'begin_time': 'begin_time',
            'geometry': 'geometry' # This will be the shapely geometry object again
        }
        
        available_columns = [k for k in column_mapping.keys() if k in gdf.columns]
        gdf_filtered = gdf[available_columns]
        
        gdf_renamed = gdf_filtered.rename(columns={k: v for k, v in column_mapping.items() if k in gdf_filtered.columns})

        # Convert GeoDataFrame to list of tuples for insertion
        # Ensure all values are serializable (strings, numbers, None)
        for _, row in gdf_renamed.iterrows():
            # Convert geometry to WKT string
            geom_wkt = None
            if 'geometry' in row and row['geometry'] is not None:
                try:
                    geom_wkt = row['geometry'].wkt
                except Exception as e:
                    log.warning(f"Could not convert geometry to WKT for a row: {e}. Setting to None.")
                    geom_wkt = None

            # Convert Timestamp objects to ISO format string
            # Check if value is a pandas Timestamp or datetime object before converting
            start_acq_date_str = None
            if 'start_acq_date' in row and row['start_acq_date'] is not None:
                if isinstance(row['start_acq_date'], (pd.Timestamp, pd.NaT)): # Handle NaT as well
                    if pd.notnull(row['start_acq_date']):
                        start_acq_date_str = row['start_acq_date'].isoformat()
                elif isinstance(row['start_acq_date'], (pd.Timestamp, pd.NaT)):
                    if pd.notnull(row['start_acq_date']):
                        start_acq_date_str = row['start_acq_date'].isoformat()
                elif isinstance(row['start_acq_date'], pd.NaT):
                    start_acq_date_str = None
                else: # Assume it's already a string or other serializable type if not Timestamp/NaT
                    start_acq_date_str = row['start_acq_date']


            row_data = (
                row.get('name', None),
                row.get('satellite_name', 'Thaichote'),
                row.get('req_name', None),
                row.get('strip_name', None),
                row.get('rev_number', None),
                start_acq_date_str, # Use the string representation
                row.get('group_path', None),
                row.get('reference_miseo', None),
                row.get('begin_time', None),
                geom_wkt # WKT string
            )
            transformed_records.append(row_data)
    
    # Push the transformed data (list of tuples with WKT geometry and ISO date strings) to XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_records)
    log.info(f"Finished transformation. Total {len(transformed_records)} records prepared for loading.")

def _load_data_to_postgres(**kwargs):
    """
    Loads the transformed data into the PostgreSQL table, deleting old data first.
    Geometry is expected to be in WKT string format, and dates/times are ISO strings.
    """
    log.info("Starting data loading to PostgreSQL...")
    # Pull the transformed data (list of tuples with WKT geometry and ISO date strings) from XCom
    transformed_records = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_geojson_data')

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

        log.info(f"Deleting existing data from {TABLE_NAME}...")
        cur.execute(f"DELETE FROM {TABLE_NAME} WHERE name IS NOT NULL")
        conn.commit()
        log.info(f"Successfully deleted existing data from {TABLE_NAME}")

        insert_query = f"""
            INSERT INTO {TABLE_NAME} (
                name, satellite_name, req_name, strip_name, rev_number,
                start_acq_date, group_path, reference_miseo, begin_time, geom
            )
            VALUES %s
            """
        log.info(f"Inserting {len(transformed_records)} records into {TABLE_NAME}...")
        # No change needed here, psycopg2 will handle ISO strings correctly for TIMESTAMP WITH TIME ZONE
        execute_values(cur, insert_query, transformed_records, template="""(
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            ST_GeomFromText(%s, 4326)
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
    dag_id="import-thaichote-from-minio-taskflow", # Keep the same DAG ID if replacing the old one
    schedule="*/5 * * * *", # ทำงานทุก 5 นาที
    start_date=pendulum.datetime(2025, 7, 4, tz="Asia/Bangkok"), # ตั้ง start_date เป็นวันที่ 4 กรกฎาคม 2568 เวลา 00:00 น. (ตามเวลาไทย)
    catchup=False,
    tags=["minio", "geojson", "postgresql", "thaichote", "etl"],
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
        task_id="extract_geojson_from_minio",
        python_callable=_extract_geojson_from_minio,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_geojson_data",
        python_callable=_transform_geojson_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task