from flask import Flask, render_template, request, redirect, url_for, session
from pymongo import MongoClient
import bcrypt
import plotly.graph_objs as go
import datetime
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
from threading import Timer
#from nom_de_votre_module import get_all_users
import json
import time
app = Flask(__name__)
app.secret_key = "mysecretkey"
client = MongoClient("mongodb://mongodb:27017/")
db = client["mydatabase"]
users_collection = db["users"]
client = MongoClient('mongodb://mongodb:27017/')
db = client['mydatabase']
collection = db['users']
time.sleep(30)
consumer = KafkaConsumer('weather', bootstrap_servers=['kafka:29092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Enregistrement des données de température et d'humidité dans la base de données
def save_weather_data(city, temp, humidity):
    now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    document = {
        #"username": username,
        "country": city,
        "temp": temp,
        "humidity": humidity,
        "date": now
    }
    collection.insert_one(document)

# Consommation des données de température et d'humidité à partir du serveur Kafka et stockage dans la base de données
def consume_weather_data():
    for message in consumer:
        #username = message.value['username']
        city = message.value['city']
        temp = message.value['temp']
        humidity = message.value['humidity']
        save_weather_data(city, temp, humidity)
        
        
consume_weather_data()
