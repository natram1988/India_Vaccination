import requests
from pandas import json_normalize
import pandas as pd
import requests
import time
from datetime import date,datetime
import os
import mysql.connector

mydb = mysql.connector.connect(
user = 'root',
password = 'stock2018',
host = '65.2.11.99',
database = 'Vaccination',
port=3306)

def get_vaccine(city_name,city_url,telegram_url):
    cursor = mydb.cursor(buffered=True)
    payload = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36', "Upgrade-Insecure-Requests": "1","DNT": "1","Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"}
    response = requests.get(city_url, headers=headers, data=payload)
    response_json_object = response.json()
    normalized_table_sessions = json_normalize(response_json_object['centers'],
    record_path=['sessions'],
    meta=['center_id', 'name', 'address', 'state_name', 'district_name', 'block_name', 'pincode', 'lat', 'long', 'from', 'to', 'fee_type'],
    errors='ignore'
    )
    queried_result = normalized_table_sessions.query('available_capacity>0 & min_age_limit==18 & available_capacity_dose1>0')
    if queried_result.empty:
        print("Response: No vaccine slots open")
        message="Response: No vaccine slots open"
        cursor.execute("INSERT INTO Telegram_messages(city_name,message) values(%s, %s)",(city_name,message))
        mydb.commit()
        new_url = message

    else:
        vaccination_centers = queried_result.loc[:,['date','name','pincode','vaccine','available_capacity']].drop_duplicates()
        table = vaccination_centers.to_string(columns = ['date','name','pincode','vaccine','available_capacity'], index = False, header = False, line_width = 70, justify = 'left')
        new_url = telegram_url+table
        cursor.execute("INSERT INTO Telegram_messages(city_name,message) values(%s, %s)",(city_name,new_url))
        mydb.commit()
        send_message(new_url,city_name)
        print(new_url)
        send_message(new_url,city_name)

def send_message(new_url,city_name):
    cursor = mydb.cursor(buffered=True)
    cursor.execute("select max(message_sent_time) from Telegram_messages where city_name='%s'" %(city_name))
    max_time = cursor.fetchone()
    cursor.execute("select message from Telegram_messages where message_sent_time='%s'" %(max_time))
    last_message = cursor.fetchone()
    last_message = str(last_message[0])
    if new_url == last_message:
        print("Message already sent")
    else:
        requests.get(new_url)

while True:
    today = date.today()
    now = datetime.now()
    now_hour = datetime.now().hour
    date_edited = today.strftime("%d-%m-%Y")
    date_time_edited = now.strftime("%d-%m-%Y %H:%M:%S")
    city1 = "Chennai"
    city_url1 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=571&date="+date_edited
    telegram_url1 = 'https://api.telegram.org/bot1761680572:AAHg3enK6v3JjMyaCaVr5UMqgmey11HN6WA/sendMessage?chat_id=-1001335725303&text='
    city2 = "Bengaluru Urban"
    city_url2 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=265&date="+date_edited
    telegram_url2 = 'https://api.telegram.org/bot1760779664:AAE_SRx9Ca5t1ytiG8zwbKeqss_eLps6x4k/sendMessage?chat_id=-1001383423441&text='
    city3 = "BBMP"
    city_url3 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=294&date="+date_edited
    telegram_url3 = 'https://api.telegram.org/bot1760779664:AAE_SRx9Ca5t1ytiG8zwbKeqss_eLps6x4k/sendMessage?chat_id=-1001383423441&text='
    print("Trying for Chennai on", date_time_edited)
    get_vaccine(city1,city_url1,telegram_url1)
    print("Trying for Bengaluru-Urban on", date_time_edited)
    get_vaccine(city2,city_url2,telegram_url2)
    print("Trying for Bengaluru-BBMP on", date_time_edited)
    get_vaccine(city3,city_url3,telegram_url3)
    time.sleep(10)
