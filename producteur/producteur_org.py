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

import json
import time
app = Flask(__name__)
app.secret_key = "mysecretkey"
client = MongoClient('mongodb://mongodb:27017/')
db = client['mydatabase']
collection = db['users']
time.sleep(30)
producer = KafkaProducer(bootstrap_servers=['kafka:29092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# Récupération de la clé d'API OpenWeatherMap
api_key = "271eef4175bdf88432b387e2ae994678"

# Récupération des données de température et d'humidité
def get_weather_data(city):
    api_key = "271eef4175bdf88432b387e2ae994678"
    url = "http://api.openweathermap.org/data/2.5/weather?"
    complete_url = url + "appid=" + api_key + "&q=" + city
    response = requests.get(complete_url)
    data = response.json()
    temper = data['main']['temp']
    temp = temper-273.15
    humidity = data['main']['humidity']
    return temp, humidity

# Envoi des données de température et d'humidité au serveur Kafka
def send_weather_data(city, temp, humidity):
    message = {"city": city, "temp": temp, "humidity": humidity}
    producer.send('weather', message)

# Récupération du pays de l'utilisateur depuis la base de données
def get_user_country(username):
    user_data = collection.find_one({"username": username})
    country = user_data['country']
    return country[0]

# Récupération des données de température et d'humidité pour le pays de l'utilisateur et envoi au serveur Kafka
def update_weather_data_for_user(username):
    country = get_user_country(username)
    temp, humidity = get_weather_data(country)
    send_weather_data(country, temp, humidity)

def update_daily_weather_data_for_all_users():
    user_list = collection.find()
    #print(*user_list)
     #print(*user_list)
    lis=user_list
    ###############################################"
    for user_data in user_list:
       if 'username' in user_data:
          print(user_data)
          username = user_data['username']
          update_weather_data_for_user(username)

    tomorrow = datetime.utcnow() + timedelta(days=1)
    midnight_tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
    seconds_until_midnight_tomorrow = (midnight_tomorrow - datetime.utcnow()).total_seconds()
    Timer(seconds_until_midnight_tomorrow, update_daily_weather_data_for_all_users).start()



# Lancement des fonctions de mise à jour et de consommation des données de température et d'humidité


update_daily_weather_data_for_all_users()

