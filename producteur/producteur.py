from pymongo import MongoClient
import requests
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta

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

# Fetch weather data for a city
def get_weather_data(city):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?appid={api_key}&q={city}"
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        temper = data['main']['temp']
        temp = temper - 273.15
        humidity = data['main']['humidity']
        return temp, humidity
    except Exception as e:
        print(f"Error fetching weather data for {city}: {e}")
        return None, None

# Send weather data to Kafka
def send_weather_data(city, temp, humidity):
    try:
        message = {"city": city, "temp": temp, "humidity": humidity}
        producer.send('weather', message)
        print(f"Weather data sent to Kafka for {city}: Temp={temp}, Humidity={humidity}")
    except Exception as e:
        print(f"Error sending weather data to Kafka for {city}: {e}")

# Get user's country from MongoDB
def get_user_country(username):
    try:
        user_data = collection.find_one({"username": username})
        if user_data and 'country' in user_data:
            country = user_data['country']
            if isinstance(country, list) and len(country) > 0:
                return country[0]
            elif isinstance(country, str):
                return country
        print(f"No valid country found for user {username}. Skipping...")
        return None
    except Exception as e:
        print(f"Error retrieving user data for {username}: {e}")
        return None

# Update weather data for a single user
def update_weather_data_for_user(username):
    country = get_user_country(username)
    if country:
        temp, humidity = get_weather_data(country)
        if temp is not None and humidity is not None:
            send_weather_data(country, temp, humidity)

# Update weather data for all users daily
def update_daily_weather_data_for_all_users():
    try:
        user_list = collection.find()
        if user_list.count() == 0:
            print("No users found in the database.")
            return

        for user_data in user_list:
            if 'username' in user_data:
                print(f"Processing user: {user_data['username']}, Country: {user_data['country']}")
                update_weather_data_for_user(user_data['username'])
    except Exception as e:
        print(f"Error updating weather data for all users: {e}")

# Schedule weather updates every 1 minute (or any interval you prefer)
def schedule_weather_updates(interval=60):  # Default interval is 60 seconds (1 minute)
    while True:
        try:
            update_daily_weather_data_for_all_users()
            print(f"Next weather update scheduled in {interval} seconds.")
            time.sleep(interval)
        except KeyboardInterrupt:
            print("Stopping weather update scheduler...")
            break

# Main execution block
if __name__ == "__main__":
    try:
        print("Starting producteur.py...")
        time.sleep(30)  # Wait for MongoDB and Kafka to start
        schedule_weather_updates(interval=60)  # Check for new weather data every 1 minute
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
