from flask import Flask, render_template, request, redirect, url_for, session
from pymongo import MongoClient
import requests
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta

# Initialize Flask app (if needed for testing)
app = Flask(__name__)
app.secret_key = "mysecretkey"

# MongoDB connection
client = MongoClient('mongodb://mongodb:27017/')
db = client['mydatabase']
collection = db['users']

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],  # Use internal Kafka port
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# OpenWeatherMap API key
api_key = "271eef4175bdf88432b387e2ae994678"

# Function to fetch weather data for a city
def get_weather_data(city):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?appid={api_key}&q={city}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        temp_kelvin = data['main']['temp']
        temp_celsius = temp_kelvin - 273.15
        humidity = data['main']['humidity']
        return temp_celsius, humidity
    except Exception as e:
        print(f"Error fetching weather data for {city}: {e}")
        return None, None

# Function to send weather data to Kafka
def send_weather_data(city, temp, humidity):
    try:
        message = {"city": city, "temp": temp, "humidity": humidity}
        producer.send('weather', message)
        print(f"Weather data sent to Kafka for {city}: Temp={temp}, Humidity={humidity}")
    except Exception as e:
        print(f"Error sending weather data to Kafka for {city}: {e}")

# Function to retrieve user's country from MongoDB
def get_user_country(username):
    try:
        user_data = collection.find_one({"username": username})
        if user_data and 'country' in user_data:
            country = user_data['country']
            if isinstance(country, list) and len(country) > 0:
                return country[0]
            elif isinstance(country, str):
                return country
        print(f"No valid country found for user {username}")
        return None
    except Exception as e:
        print(f"Error retrieving user data for {username}: {e}")
        return None

# Function to update weather data for all users
def update_weather_data_for_all_users():
    try:
        user_list = collection.find()
        if user_list.count() == 0:
            print("No users found in the database.")
            return

        for user_data in user_list:
            if 'username' in user_data and 'country' in user_data:
                username = user_data['username']
                country = user_data['country']
                if isinstance(country, list) and len(country) > 0:
                    country = country[0]
                elif isinstance(country, str):
                    country = country
                else:
                    print(f"Invalid country field for user {username}. Skipping...")
                    continue

                print(f"Processing user: {username}, Country: {country}")
                temp, humidity = get_weather_data(country)
                if temp is not None and humidity is not None:
                    send_weather_data(country, temp, humidity)
    except Exception as e:
        print(f"Error updating weather data for all users: {e}")

# Schedule weather data updates
def schedule_weather_updates(interval=60):  # Default interval is 1 minute
    while True:
        try:
            update_weather_data_for_all_users()
            print(f"Next weather update scheduled in {interval} seconds.")
            time.sleep(interval)
        except KeyboardInterrupt:
            print("Stopping weather update scheduler...")
            break

# Main execution block
if __name__ == "__main__":
    try:
        print("Starting producteur.py...")
        # Wait for MongoDB and Kafka to start
        time.sleep(30)
        # Start scheduling weather updates
        schedule_weather_updates(interval=60)  # Check for new weather every 1 minute
    except Exception as e:
        print(f"An unexpected error occurred: {e}")