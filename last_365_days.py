import requests
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error


# Database connection settings
DB_CONFIG = {
    'host': 'localhost',
    'database': 'weatherschema',
    'user': 'root',
    'password': 'Janushan-123'
}

API_KEY = "07d4c6c170184cdf819202311242109"
LOCATION = 'London'
num_days = 365  # Fetch data for 365 days
batch_size = 50  # Number of rows to insert in one batch

def connection_to_db():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None


def insert_many(cursor, batch_data):
    try:
        insert_query = """
                INSERT INTO weatherdatatable (Date, Avg_Temp, Weather_Description)
                VALUES (%s, %s, %s)
            """
        cursor.executemany(insert_query,batch_data)

    except Error as e:
        print(f"Error inserting data: {e}")


# Function to get weather data from the API
def fetch_weather_data(date):
    url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={LOCATION}&dt={date}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data for date {date} (status code: {response.status_code})")
    except requests.RequestException as e:
        print(f"Request error: {e}")
    return None

def process_weather_data(data):
    
    weather_entries = []
    
    forecastdata = data["forecast"]["forecastday"]
    for day in forecastdata:
        weather_entries = []
        date = day["date"]
        avg_temp = day["day"]["avgtemp_c"]
        weather_desc = day["day"]["condition"]["text"]
        date = date.replace("-","") 
        
        weather_entries.append((date,avg_temp,weather_desc))

    return weather_entries



######## MAIN FUNCTION TO FETCH AND INSERT WEATHER DATA FOR MULTIPLE DATES ##########



def main():
    start_date = datetime.now() - timedelta(days = num_days)
    date_list = [(start_date + timedelta(days = i)).strftime('%Y-%m-%d') for i in range(num_days)]
    
    connection = connection_to_db()
    if connection is None:
        return 
    
    try:
        cursor = connection.cursor()
        
        batch_data = [] #initialise empty list for batch data insertion
        
        for i, date in enumerate(date_list): # date in enumerate date list gets the dates in the list and gets an associated index, assigned to i
            weather_data = fetch_weather_data(date)
            if weather_data:
                processed_data = process_weather_data(weather_data)
                batch_data.extend(processed_data)
                
                #insert data in batches
                if len(batch_data) >= batch_size:
                     insert_many(cursor, batch_data)
                     batch_data.clear() # clear array for new batch
                     print("Batch Sent")
        
        if batch_data:
            insert_many(cursor, batch_data)
            
        #commit transaction
        connection.commit()
                    
    except Error as e:
        print(f"Database error: {e}")
        
    finally:
        #close connection
        cursor.close()
        connection.close()
        print("MySQL connection closed.")


#run script

if __name__ == "__main__":
    main()









