from flask import Flask, render_template, request, redirect, url_for, session
from pymongo import MongoClient
import bcrypt
import time
import plotly.graph_objs as go
import datetime
import requests
from datetime import datetime, timedelta
from pymongo import MongoClient
from kafka import KafkaProducer, KafkaConsumer
from threading import Timer
import geocoder    
import os 
#from app import app
#from nom_de_votre_module import get_all_users
import json

app = Flask(__name__)
app.secret_key = "mysecretkey"

client = MongoClient("mongodb://mongodb:27017/")
db = client["mydatabase"]
users_collection = db["users"]
client = MongoClient('mongodb://mongodb:27017/')
db = client['mydatabase']
collection = db['users']

# Connexion au serveur Kafka
time.sleep(30)
producer = KafkaProducer(bootstrap_servers=['kafka:29092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer('weather', bootstrap_servers=['kafka:29092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Récupération de la clé d'API OpenWeatherMap
api_key = "271eef4175bdf88432b387e2ae994678"

# Récupération des données de température et d'humidité

def get_weather_data(ip_address):
    ip_response = requests.get('https://api.ipify.org?format=json')
    ip_data = ip_response.json()
    ip_address = ip_data['ip']

    ip_api_url = f'http://ip-api.com/json/{ip_address}'
    ip_api_response = requests.get(ip_api_url)
    ip_api_data = ip_api_response.json()

    city = ip_api_data['city']
    country = ip_api_data['country']
    latitude = ip_api_data['lat']
    longitude = ip_api_data['lon']

    weather_response = requests.get(f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid=271eef4175bdf88432b387e2ae994678&units=metric')
    weather_data = weather_response.json()

    temp = weather_data['main']['temp']
    humidity = weather_data['main']['humidity']
    pressure=weather_data['main']['pressure']

    now = datetime.utcnow()
    document = {
        "ip": ip_address,
        "city": city,
        "country": country,
        "latitude": latitude,
        "longitude": longitude,
        "temp": temp,
        "humidity": humidity,
        "pressure":pressure,
        "date": now
    }
    collection.insert_one(document)

    return document
def get_location_data(ip_address):
    # Utiliser le module geocoder pour obtenir les données de position
    g = geocoder.ip(ip_address)
    location_data = {
        'latitude': g.lat,
        'longitude': g.lng,
        'city': g.city,
        'country': g.country
    }
    return location_data
#def get_weather_data(city):
 #   api_key = "271eef4175bdf88432b387e2ae994678"
  #  url = "http://api.openweathermap.org/data/2.5/weather?"
   # complete_url = url + "appid=" + api_key + "&q=" + city
    #response = requests.get(complete_url)
    #data = response.json()
    #temp = data['main']['temp']
    #humidity = data['main']['humidity']
    #return temp, humidity

# Envoi des données de température et d'humidité au serveur Kafka
#def send_weather_data(city, temp, humidity):
 #   message = {"city": city, "temp": temp, "humidity": humidity}
  #  producer.send('weather', message)

# Récupération du pays de l'utilisateur depuis la base de données
#def get_user_country(username):
 #   user_data = collection.find_one({"username": username})
  #  country = user_data['country']
   # return country[0]
#
# Récupération des données de température et d'humidité pour le pays de l'utilisateur et envoi au serveur Kafka
#def update_weather_data_for_user(username):
 #   country = get_user_country(username)
  #  temp, humidity = get_weather_data(country)
   # send_weather_data(country, temp, humidity)
#
# Enregistrement des données de température et d'humidité dans la base de données
#def save_weather_data(city, temp, humidity):
  #  now = datetime.utcnow()
 #   document = {
   #     "city": city,
    #    "temp": temp,
     #   "humidity": humidity,
      #  "date": now
    #}
#    collection.insert_one(document)

# Consommation des données de température et d'humidité à partir du serveur Kafka et stockage dans la base de données
#def consume_weather_data():
 #   for message in consumer:
  #      city = message.value['city']
   #     temp = message.value['temp']
    #    humidity = message.value['humidity']
     #   save_weather_data(city, temp, humidity)

# Mise à jour des données de température et d'humidité pour tous les utilisateurs une fois par jour
#def update_daily_weather_data_for_all_users():
 #   user_list = collection.find()
  #  ###############################################"
    #for user_data in user_list:
        #usernam = user_data['username']
#    update_weather_data_for_user("seif")

 #   tomorrow = datetime.utcnow() + timedelta(days=1)
  #  midnight_tomorrow = datetime(year=tomorrow.year, month=tomorrow.month, day=tomorrow.day, hour=0, minute=0, second=0)
   # seconds_until_midnight_tomorrow = (midnight_tomorrow - datetime.utcnow()).total_seconds()
    #Timer(seconds_until_midnight_tomorrow, update_daily_weather_data_for_all_users).start()

#

# Lancement des fonctions de mise à jour et de consommation des données de température et d'humidité
@app.route('/historique')
def historique():
    nom_utilisateur = session["username"]
    print(nom_utilisateur)
    # obtenir le pays de l'utilisateur à partir de la base de données
    utilisateurs = users_collection.find({"username": nom_utilisateur})
    historique = []

    for utilisateur in utilisateurs:
        # obtenir le pays de l'utilisateur
        country = utilisateur.get('country')
        print(country[0])
        if country:
            # rechercher tous les documents qui ont le même pays que l'utilisateur
            historique.extend(list(collection.find({'country': country[0], 'username': {'$exists': False}, 'ip': {'$exists': False}})))


    # renvoyer le rendu HTML avec les données d'historique
    return render_template('historique.html', historique=historique)


@app.route('/weather')
def weather():
    city = request.args.get('city')
    api_key = '271eef4175bdf88432b387e2ae994678'  # Remplacer YOUR_API_KEY par votre clé API
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    response = requests.get(url)
    weather_data = response.json()
    temperatur = round(weather_data['main']['temp'] - 273.15, 1)
    humidit = weather_data['main']['humidity']
    pressure=weather_data['main']['pressure']
    return {'temperatur':temperatur, 'humidit': humidit , 'pressure':pressure}
    #f"La température à {city} est de {temperatur}°C et l'humidité est de {humidit}%"


    
@app.route('/',methods=["GET","POST"])
def index():
    cityweather=''
    error = 0
    cityName = ''
    if request.method == "POST":       
        cityName = request.form.get("cityName")  
        if cityName:
           weather_url = f'https://api.openweathermap.org/data/2.5/weather?lat=44.34&lon=10.99&appid={api_key}'
           response = requests.get(weather_url)
           x = response.json()
           y = x["coord"]
           z = x["main"]
           a = x["weather"]
           b = x["sys"]
           lon=y["lon"]
           lat=y["lat"]
           temperature = round(z["temp"]-273.15,1)
           humidty = z["humidity"]
           pressure= z["pressure"]
           description = a[0]["description"]
           country = b["country"] 
           cityweather = {
            'lat': str(lat),
            'lon': str(lon),
            'temperature': str(temperature),
            'humidity': str(humidty),
            'pressure': str(pressure),
            'description': str(description),
            'cityname': str(cityName)
           }
    return  render_template("index.html", cityweather=cityweather)
    
    
    
    
    

@app.route("/home", methods=["GET", "POST"])
def home():
    if "username" in session:
        #weather_data
        humidity=0
        pressure=0
        place=0
        #if request.method == 'POST':
# Obtenir l'adresse IP de l'utilisateur
        # Obtenir l'adresse IP de l'utilisateur
        ip_address = request.remote_addr

    # Obtenir les données météorologiques à partir de l'adresse IP
        weather_data = get_weather_data(ip_address)

    # Obtenir la position géographique à partir de l'adresse IP
        location_data = get_location_data(ip_address)
        latitude = location_data['latitude']
        longitude = location_data['longitude']

    # Rendre le template HTML en passant les données météorologiques et la position géographique
        return render_template('map.html', temperature=weather_data['temp'], humidity=weather_data['humidity'], pressure=weather_data['pressure'], latitude=weather_data['latitude'], longitude=weather_data['longitude'],city=weather_data['city'] )
    else:
        return redirect(url_for("login"))

@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        existing_user = users_collection.find_one({"email": request.form["email"]})

        if existing_user is None:
            hashpass = bcrypt.hashpw(request.form["password"].encode("utf-8"), bcrypt.gensalt())
            country = request.form.getlist("country")
            users_collection.insert_one({"username": request.form["username"], "email": request.form["email"], "password": hashpass, "country": country})
            session["username"] = request.form["username"]
            return redirect(url_for("home"))
        else:
            return "This email already exists in the database."
    else:
        return render_template("signup.html")

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = users_collection.find_one({"email": request.form["email"]})

        if user:
            if bcrypt.checkpw(request.form["password"].encode("utf-8"), user["password"]):
                session["username"] = user["username"]
                return redirect(url_for("home"))
            else:
                return "Invalid email or password."
        else:
            return "Invalid email or password."
    else:
        return render_template("login.html")
    
    
    
    

        ####################################################################
#####################################################################################""

# Connexion à la base de données


# Connexion à la base de données
#update_daily_weather_data_for_all_users()
#consume_weather_data()

if __name__ == "__main__":
    port = int ( os.environ.get( 'PORT' , 5000 )) 
    app . run ( debug = True , host = '0.0.0.0' , port = port ) 

