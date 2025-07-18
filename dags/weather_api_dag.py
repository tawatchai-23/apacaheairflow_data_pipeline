# from airflow import DAG
# from airflow.models import Variable  
# from airflow.operators.email import EmailOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils import timezone
# from datetime import timedelta
# import requests
# import json

# def _get_weather_data():
#     # API_KEY = "bc1888a6cdae32f4e6688e7d5d8f7fd1"
#     # API_KEY = os.environ.get("WEATHER_API_KEY")
#     API_KEY = Variable.get("weather_api_key")

#     payload = {
#         "q": "bangkok",
#         "appid": API_KEY,
#         "units": "metric"
#     }
#     url = "https://api.openweathermap.org/data/2.5/weather"
#     response = requests.get(url, params=payload)
#     print(response.url)

#     data = response.json()
#     print(data)

#     with open("/opt/airflow/dags/data.json", "w") as f:
#         json.dump(data, f)

# def _get_validate_data():
#     with open("/opt/airflow/dags/data.json", "r") as f:
#         data = json.load(f)

#     assert data.get("main") is not None

# def _create_weather_table():
#     pg_hook = PostgresHook(
#         postgres_conn_id="weather_postgres_conn",
#         schema="postgres"
#     )
#     connection = pg_hook.get_conn()
#     cursor = connection.cursor()

#     sql = """
#         CREATE TABLE IF NOT EXISTS weathers (
#             dt BIGINT NOT NULL,
#             temp FLOAT NOT NULL
#         )
#     """
#     cursor.execute(sql)
#     connection.commit()

# def _load_data_to_postgres():
#     pg_hook = PostgresHook(
#         postgres_conn_id="weather_postgres_conn",
#         schema="postgres"
#     )
#     connection = pg_hook.get_conn()
#     cursor = connection.cursor()

#     with open("/opt/airflow/dags/data.json", "r") as f:
#         data = json.load(f)

#     temp = data["main"]["temp"]
#     dt = data["dt"]
#     sql = f"""
#         INSERT INTO weathers (dt, temp) VALUES ({dt}, {temp})
#     """
#     cursor.execute(sql)
#     connection.commit()

# default_args = {
#     "email": ["kan@odds.team"],
#     "retries": 3,
#     "retry_delay": timedelta(minutes=1),
# }  

# with DAG(
#     "weather_api_dag",
#     default_args=default_args,
#     schedule="0 */3 * * *",
#     start_date=timezone.datetime(2025, 2, 1),
#     tags=["joke","lolo","my_first_dag"],
# ):
#     start = EmptyOperator(task_id="start")

#     get_weather_data = PythonOperator(
#         task_id="get_weather_data",
#         python_callable=_get_weather_data,
#     )

#     validate_data = PythonOperator(
#         task_id="get_validate_data",
#         python_callable=_get_validate_data,
#     )

#     create_weather_table = PythonOperator(
#         task_id="create_weather_table",
#         python_callable=_create_weather_table,
#     )

#     load_data_to_postgres = PythonOperator(
#         task_id="load_data_to_postgres",
#         python_callable=_load_data_to_postgres,
#     )

#     send_email = EmailOperator(
#         task_id="send_email",
#         to=["kan@odds.team"],
#         subject="สำเร็จแล้วไอ้เหี้ย",
#         html_content="Done"
#     )
        
#     end = EmptyOperator(task_id="end")

#     start >> get_weather_data >> validate_data >> create_weather_table >> load_data_to_postgres >> end
#     load_data_to_postgres >> send_email