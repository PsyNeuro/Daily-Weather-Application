#get forecast data for days ONLY three days in advance 

import requests

API_KEY = "07d4c6c170184cdf819202311242109"
LOCATION = 'London'
no_days = "3"

def fetch_weather_data(no_days):
    url = f"http://api.weatherapi.com/v1/forecast.json?key={API_KEY}&q={LOCATION}&days={no_days}&aqi=no&alerts=no"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data for date {no_days} days in advance (status code: {response.status_code})")
    except requests.RequestException as e:
        print(f"Request error: {e}")
    return None

x = fetch_weather_data(3)
forecast_3_days = x['forecast']['forecastday'][1]
print(forecast_3_days)