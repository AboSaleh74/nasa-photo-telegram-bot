from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago
from io import BytesIO
import base64
import os
import requests
import json

with DAG (
    dag_id='etlpipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    ##step 1 : Create table in postgres if not exists
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        ##create table query
        sql_query = """
        CREATE TABLE IF NOT EXISTS apod_table (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url VARCHAR(255),
            media_type VARCHAR(50),
            date DATE,
            image_data BYTEA
        );
        """
        
        ##execute the query
        postgres_hook.run(sql_query)
        print("Table created successfully")
    
    ##step 2 : Extract data from API
    url = "https://api.nasa.gov/planetary/apod?api_key=q3NTrYFRsX3YJ8VP7LganZ6LkGCgBoVpGGX0OXWO"
    extract_apod= SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        method='GET',
        data = {"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"},
        response_filter=lambda response: response.json()
    )
    
    ##step 3 : Transform data
    @task
    def transform_data(response):
        image_url = response.get('url', '')
        image_data = None

        if response.get('media_type') == 'image' and image_url:
            res = requests.get(image_url)
            if res.status_code == 200:
                image_data = BytesIO(res.content).getvalue()
                # تحويل الصورة إلى Base64
                image_data_base64 = base64.b64encode(image_data).decode('utf-8')

        transformed_data = {
            'title': response.get('title', ' '),
            'explanation': response.get('explanation', ' '),
            'url': image_url,
            'media_type': response.get('media_type', ' '),
            'date': response.get('date', ' '),
            'image_data': image_data_base64  # تخزين الصورة كـ Base64
        }
        return transformed_data
    
    
    ##step 4 : Load data into postgres
    @task
    def load_data_to_postgres(data):
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        insert_query = """
        INSERT INTO apod_table (title, explanation, url, media_type, date,image_data)
        VALUES (%s, %s, %s, %s, %s,%s);
        """
        
        postgres_hook.run(insert_query, parameters=(data['title'],
                                                    data['explanation'],
                                                    data['url'],
                                                    data['media_type'],
                                                    data['date'],
                                                    data['image_data']))
        print("Data loaded successfully")
    @task
    def send_to_telegram(data):
        bot_token = "7224671792:AAESNbHbpJeibGQQzgKtGswL0KedMpGNIBY"
        chat_id = "1102308046"

        caption = f"📅 {data['date']}\n📌 {data['title']}\n\n{data['explanation'][:1000]}..."

        image_data_bytes = base64.b64decode(data['image_data'])
        
        # إرسال الصورة كـ ملف
        response = requests.post(
            url=f"https://api.telegram.org/bot{bot_token}/sendPhoto",
            data={
                "chat_id": chat_id,
                "caption": caption
            },
            files={
                "photo": ("nasa_apod.jpg",image_data_bytes)
            }
        )

        if response.status_code == 200:
            print("✅ Photo sent to Telegram successfully")
        else:
            print(f"❌ Failed to send photo: {response.text}")

    
    ##step 5 :  verfiy data by DBviewer
    
    ##step 6 :  define task dependencies
    create_table() >> extract_apod 
    api_response = extract_apod.output
    transformed_data = transform_data(api_response)
    loaded = load_data_to_postgres(transformed_data)
    loaded >> send_to_telegram(transformed_data)