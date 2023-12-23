from datetime import datetime
import time
import uuid
# from airflow import DAG
# from airflow.operators.python import PythonOperator


def get_data(city_name):
    import requests
    import os
    from dotenv import load_dotenv
    load_dotenv()
    openweathermap_api_endpoint ="https://api.openweathermap.org/data/2.5/weather?q="+city_name+"&appid="+os.getenv("API_KEY")+"&units=metric&lang=en"
    res = requests.get(openweathermap_api_endpoint)
    res = res.json()

    return res

def format_data(res):
    unique_id = str(uuid.uuid4())
    city = res['name']
    temp = res["main"]["temp"]
    humidity = res["main"]["humidity"]
    weather= res['weather'][0]['description']
    
    json_message = {"unique_id": unique_id, "city":city, "weather":weather, "temperature": temp, "humidity": humidity, "creation_time": time.strftime("%Y-%m-%d %H:%M:%S")}
    return json_message

def stream_data():
    import json
    from kafka import KafkaProducer
    import logging
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    while True:
        try:
            res = get_data(city_name = "Bangkok")
            res = format_data(res)
            producer.send('weather', json.dumps(res).encode('utf-8'))
            print("Published Message")
            
            res = get_data(city_name = "Tokyo")
            res = format_data(res)
            producer.send('weather', json.dumps(res).encode('utf-8'))
            print("Published Message")

            res = get_data(city_name = "Berlin")
            res = format_data(res)
            producer.send('weather', json.dumps(res).encode('utf-8'))
            print("Published Message")
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

stream_data()