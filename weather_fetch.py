#retrival of data from the weatherdata table
#connect to DB

import mysql.connector
from mysql.connector import Error

DB_CONFIG = {
    'host': 'localhost',
    'database': 'weatherschema',
    'user': 'root',
    'password': 'Janushan-123'
}


#establish connection with DB
def connection_to_db():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None


#get input from user
def get_input():
        
    while True:
        user_input = input("\nWhich date would you like to get the weather for? (yyyy-mm-dd format please)\n Or Type 'close' to Close application\n")
        if len(user_input) == 10 and user_input[4] == "-" and user_input[7] == "-":
            input_clean = user_input.replace("-", "")
            return input_clean
        elif user_input.lower() == "close":
            return "close"
        else:
            print("Invalid format. Please enter the date in yyyy-mm-dd format.")
    

#fetch the data from the SQL db using the date
def sql_fetch(cursor,input_clean):
    
    try:
        sql = f"SELECT * FROM weatherdatatable WHERE Date = %s"
        cursor.execute(sql,(input_clean,)) # SQL injections are prevented by the cursor execute funciton, so long as you give it the parameters using %s and input clean 
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"\nAn error occured {e}\n")
        return None

#print output
def print_output(result,user_input):
    
    if result:
        for row in result:
            print(f"\nDate: {user_input} --> Temp: {row[1]}°C, Description: {row[2]}\n")
    else:
        print(f"\nNo weather data found for date {user_input}\n")


#main code
def main():
    
    connection = connection_to_db()
    if connection is None:
        return 
    
    try:
        cursor = connection.cursor()
        
        while True:
            date = get_input()
            if date == "close":
                return
            
            result = sql_fetch(cursor, date)
            
            if result: 
                print_output(result, date)
                
            else:
                print("No records for that date exist, please try again")
        
        
    except Error as e:
        
        print(f"\nAn error occured: {e}\n")
        
    finally:
        
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()
        print("\nMySQL connection closed.\n")
        


if __name__ == "__main__":
    main()
