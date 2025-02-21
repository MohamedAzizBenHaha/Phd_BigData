from flask import Flask, render_template, request, redirect, url_for, session
from pymongo import MongoClient
import bcrypt
import time
import requests
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import json
import os

app = Flask(__name__)
app.secret_key = "mysecretkey"

# MongoDB connection
client = MongoClient("mongodb://mongodb:27017/")
db = client["mydatabase"]
users_collection = db["users"]
weather_collection = db["weather"]

# Kafka producer setup
time.sleep(30)  # Wait for Kafka to start
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# OpenWeatherMap API key
api_key = "271eef4175bdf88432b387e2ae994678"

# Function to fetch weather data for a given city
def get_weather_data(city):
    try:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        temp = data['main']['temp']
        humidity = data['main']['humidity']
        pressure = data['main']['pressure']
        description = data['weather'][0]['description']
        latitude = data['coord']['lat']
        longitude = data['coord']['lon']

        now = datetime.utcnow().isoformat()  # Convert datetime to ISO 8601 string
        document = {
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "temp": temp,
            "humidity": humidity,
            "pressure": pressure,
            "description": description,
            "date": now
        }
        weather_collection.insert_one(document)  # Save to MongoDB
        producer.send('weather', document)  # Send to Kafka
        return document
    except Exception as e:
        print(f"Error fetching weather data for {city}: {e}")
        return None

# Home route (dashboard for logged-in users)
@app.route("/home", methods=["GET", "POST"])
def home():
    if "username" in session:
        username = session["username"]
        user = users_collection.find_one({"email": username})  # Find user by email
        if user and "country" in user:
            # Extract the first country from the list
            country = user["country"][0] if isinstance(user["country"], list) else user["country"]

            # Fetch weather data for the user's country
            weather_data = get_weather_data(country)
            if weather_data:
                return render_template(
                    'home.html',
                    temperature=weather_data['temp'],
                    humidity=weather_data['humidity'],
                    pressure=weather_data['pressure'],
                    city=weather_data['city'],
                    latitude=weather_data['latitude'],
                    longitude=weather_data['longitude']
                )
            else:
                return "Error fetching weather data for your location."
        return "No country information available for your account."
    else:
        return redirect(url_for("login"))

# Index route (main page)
@app.route('/', methods=["GET", "POST"])
def index():
    cityweather = ''
    error = 0
    if request.method == "POST":
        cityName = request.form.get("cityName")
        if cityName:
            weather_url = f'https://api.openweathermap.org/data/2.5/weather?q={cityName}&appid={api_key}&units=metric'
            response = requests.get(weather_url)
            if response.status_code == 200:
                x = response.json()
                y = x["coord"]
                z = x["main"]
                a = x["weather"]
                b = x["sys"]
                lon = y["lon"]
                lat = y["lat"]
                temperature = round(z["temp"], 1)
                humidity = z["humidity"]
                pressure = z["pressure"]
                description = a[0]["description"]
                country = b["country"]

                cityweather = {
                    'lat': str(lat),
                    'lon': str(lon),
                    'temperature': str(temperature),
                    'humidity': str(humidity),
                    'pressure': str(pressure),
                    'description': str(description),
                    'cityname': str(cityName)
                }
            else:
                error = 1
    return render_template("index.html", cityweather=cityweather, error=error)

# Signup route
@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        existing_user = users_collection.find_one({"email": request.form["email"]})
        if existing_user is None:
            hashpass = bcrypt.hashpw(request.form["password"].encode("utf-8"), bcrypt.gensalt())
            country = request.form.getlist("country")  # Get the selected country
            users_collection.insert_one({
                "username": request.form["username"],
                "email": request.form["email"],
                "password": hashpass,
                "country": country
            })
            session["username"] = request.form["username"]
            return redirect(url_for("home"))
        else:
            return "This email already exists in the database."
    return render_template("signup.html")

# Login route
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = users_collection.find_one({"email": request.form["email"]})
        if user and bcrypt.checkpw(request.form["password"].encode("utf-8"), user["password"]):
            session["username"] = user["username"]
            return redirect(url_for("home"))
        else:
            return "Invalid email or password."
    return render_template("login.html")

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
