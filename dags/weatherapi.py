import requests
import json
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.cloud import storage
from google.oauth2.service_account import Credentials

doc_md = """
Essa DAG é responsável por capturar os dados da API de clima e armazenar no Storage.
"""

dag_owner = 'data-engineer-team'

default_args = {'owner': dag_owner,
                'depends_on_past': False,
                'retries': None,
                'retry_delay': timedelta(minutes=5)
                }

with DAG(dag_id='weather_api',
         default_args=default_args,
         description='DAG da Weather API',
         doc_md=doc_md,
         start_date=datetime(year=2023, month=5, day=11),
         schedule_interval='0 0 * * *',
         catchup=False,
         tags=['weather', 'api']) as dag:

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[logging.StreamHandler()]
    )

    def get_remote_path(bucket_name: str, folder: str, partition_field: datetime, file_name: str) -> str:
        return f"{folder}/partition_field={partition_field.strftime('%Y-%m-%d')}/{file_name}"

    def upload_to_bucket(data: str, bucket_name: str, remote_path: str) -> None:
        credentials = Credentials.from_service_account_info(
            json.loads(Variable.get('gcp_storage_key')))
        client = storage.Client(credentials=credentials)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(remote_path)
        blob.upload_from_string(data, content_type='text/json')

    def get_weather_data(api_key: str, city: str, date: datetime) -> dict:
        formatted_date = date.strftime('%Y-%m-%d')
        url = f'http://api.weatherapi.com/v1/history.json?key={api_key}&q={city}&dt={formatted_date}'
        response = requests.get(url)
        if response.status_code != 200:
            logging.warning(
                f'Request for date {formatted_date} failed with status code {response.status_code}')
            return None
        data = response.json()
        return data

    def load_weather_data(api_key: str, city: str, start_date: datetime, num_days: int, bucket_name: str, folder: str):
        for i in range(num_days):
            if num_days > 1:
                date = start_date + timedelta(days=i)
            else:
                date = start_date
            data = get_weather_data(api_key, city, date)
            if data is None:
                continue
            json_data = json.dumps(data)
            remote_path = get_remote_path(
                bucket_name, folder, date, f"{data['forecast']['forecastday'][0]['date_epoch']}.json")
            upload_to_bucket(json_data, bucket_name, remote_path)
            logging.info(
                f'Data for {date.strftime("%Y-%m-%d")} uploaded to {bucket_name}/{remote_path}')

    task1 = PythonOperator(
        task_id='load_weather_data',
        python_callable=load_weather_data,
        op_kwargs={
            'api_key': '6d6d0c5bc873417ab0510523230805',
            'city': 'São_Paulo',
            'start_date': datetime(2023, 5, 10),
            'num_days': 1,
            'bucket_name': 'weather-api-raw',
            'folder': 'raw'
        },
        dag=dag
    )


# Tratamento desses dados em SQL
sql = """
SELECT 
  hour.uv,
  hour.gust_kph,
  hour.gust_mph,
  hour.vis_miles,
  hour.chance_of_rain,
  hour.dewpoint_f,
  hour.heatindex_c,
  hour.temp_c,
  hour.precip_mm,
  hour.windchill_f,
  hour.windchill_c,
  hour.wind_dir,
  hour.feelslike_f,
  hour.time,
  hour.pressure_in,
  hour.time_epoch,
  hour.humidity,
  hour.pressure_mb,
  hour.will_it_snow,
  hour.wind_degree,
  hour.cloud,
  hour.wind_kph,
  hour.heatindex_f,
  hour.wind_mph,
  hour.vis_km,
  hour.feelslike_c,
  hour.dewpoint_c,
  hour.precip_in,
  hour.will_it_rain,
  hour.is_day,
  hour.temp_f,
  hour.chance_of_snow,
  condition.code,
  condition.icon,
  condition.text,
  astro.moonset,		
  astro.moon_phase,
  astro.moonrise,			
  astro.sunset,			
  astro.moon_illumination,		
  astro.sunrise,
  day.uv AS day_uv,
  day.condition.code AS day_code,
  day.condition.icon AS day_icon,
  day.condition.text AS day_text,
  day.uv AS day_uv,		 
  day.avghumidity AS day_avghumidity,		
  day.avgvis_miles AS day_avgvis_miles,				
  day.avgvis_km	AS day_avgvis_km,		
  day.totalprecip_mm AS day_totalprecip_mm,				
  day.maxwind_kph AS day_maxwind_kph,			
  day.totalprecip_in AS day_totalprecip_in,	
  day.avgtemp_f AS day_avgtemp_f,			
  day.maxtemp_f AS day_maxtemp_f,			
  day.avgtemp_c AS day_avgtemp_c,				
  day.maxwind_mph AS day_maxwind_mph,			
  day.mintemp_c AS day_mintemp_c,				
  day.mintemp_f AS day_mintemp_f,		
  day.maxtemp_c AS day_maxtemp_c,
  forecastday.date_epoch,
  forecastday.date,
  location.localtime_epoch,
  location.country,
  location.localtime,
  location.lat,
  location.region,
  location.tz_id,
  location.lon,
  location.name
FROM 
  `formal-analyzer-386102.weather_api.weather`,
   UNNEST(forecast.forecastday) AS forecastday,
   UNNEST(forecastday.hour) AS hour
"""
