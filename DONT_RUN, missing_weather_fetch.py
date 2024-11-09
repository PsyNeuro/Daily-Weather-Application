#if we last collelcted data 60 days ago, the db will have the last 60 date entries missing 
# fetch the int list of dates from the SQL in the format  yyyymmdd no dashes
# find which are missing, fetch them from the API 

#THIS wont actually work on a free acount, but i'm doing it just for practice

import requests
import pandas as pd
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    'host': 'localhost',
    'database': 'weatherschema',
    'user': 'root',
    'password': 'Janushan-123'
}

API_KEY = "07d4c6c170184cdf819202311242109"
LOCATION = 'London'
batch_size = 50  # Number of rows to insert in one batch

#establish connection with DB
def connection_to_db():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

#get all the dates from the DB
def fetch_dates(cursor):
    try:
        sql = "SELECT Date FROM weatherdatatable"
        cursor.execute(sql)
        result = cursor.fetchall() # this is a list of tuples, so for a group of date like d1, d2, d3 ..., it outputs (d1,),(d2,),(d3,), and so on, this is a tuple 
                                   #so we need to flatten it into a list. 
        return [r[0] for r in result] # r in result r[i][0], select tuple i, and select the first value in it, 0                     
        
    except Error as e:
        print(f"\nAn error occured {e}\n")
        return None

#calculate which are missing in the range dates[0] and today
def missing_dates(date_list):
    date_list.sort(reverse=False)

    date_list = pd.to_datetime(date_list, format='%Y%m%d') # parse strings into date format
    fullrange = pd.date_range(start=date_list.min(), end=datetime.now().date(), freq='D') # remember this is a pandas range we've made, so we need to parse the strings from SQL into dates and and PD
    
    missing_dates = fullrange.difference(date_list)
    print(f"{len(missing_dates)} entries have been added to the DB")
    return  missing_dates

#fetch infomraiton from the api 
def fetch_weather_data(date):
    date_str = date.strftime('%Y-%m-%d')
    url = f"http://api.weatherapi.com/v1/history.json?key={API_KEY}&q={LOCATION}&dt={date_str}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data for date {date} (status code: {response.status_code})")
    except requests.RequestException as e:
        print(f"Request error: {e}")
    return None

#process that JSON 
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

#inset the processed data into the DB
def insert_many(cursor, batch_data):
    try:
        insert_query = """
                INSERT INTO weatherdatatable (Date, Avg_Temp, Weather_Description)
                VALUES (%s, %s, %s)
            """
        cursor.executemany(insert_query,batch_data)

    except Error as e:
        print(f"Error inserting data: {e}")
        

# Main function

def main():
    
    connection = connection_to_db()
    if connection is None:
        return 
    
    try:
        cursor = connection.cursor()
        date_list = fetch_dates(cursor)
        missing_dates(date_list)
        
        batch_data = [] #initialise empty list for batch data insertion
        
        
        # for i, date in enumerate(date_list): # date in enumerate date list gets the dates in the list and gets an associated index, assigned to i
        #     weather_data = fetch_weather_data(date)
        #     if weather_data:
        #         processed_data = process_weather_data(weather_data)
        #         batch_data.extend(processed_data)
                
        #         #insert data in batches
        #         if len(batch_data) >= batch_size:
        #              insert_many(cursor, batch_data)
        #              batch_data.clear() # clear array for new batch
        #              print("Batch Sent")
        
        # if batch_data:
        #     insert_many(cursor, batch_data)
            
        #commit transaction
        connection.commit()
                    
    except Error as e:
        print(f"Database error: {e}")
        
    finally:
        #close connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("MySQL connection closed.")


#run script

if __name__ == "__main__":
    main()