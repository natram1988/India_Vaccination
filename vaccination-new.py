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
database = 'GetJabbed',
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
        message = "Response: No vaccine slots open"
        message_status = "Not sent"
        cursor.execute("INSERT INTO Telegram_messages(city_name,message,message_status) values(%s, %s, %s)",(city_name,message,message_status))
        mydb.commit()
        new_url = message
    else:
        vaccination_centers = queried_result.loc[:,['date','name','pincode','vaccine','available_capacity']].drop_duplicates()
        table = vaccination_centers.to_string(columns = ['date','name','pincode','vaccine','available_capacity'], index = False, header = False, line_width = 70, justify = 'left')
        new_url = telegram_url+table
        print(new_url)
        cursor = mydb.cursor(buffered=True)
        cursor.execute("select message from Telegram_messages where message_sent_time=(select max(message_sent_time) from Telegram_messages where city_name='%s') and city_name='%s'" %(city_name, city_name))
        last_message = cursor.fetchone()
        last_message = str(last_message[0])
        print("Last message is ",last_message)
        if (last_message == new_url):
            print("Message already sent")
            message_status = "Not sent"
            cursor.execute("INSERT INTO Telegram_messages(city_name,message,message_status) values(%s, %s, %s)",(city_name,new_url,message_status))
            mydb.commit()
        else:
            message_status = "Sent"
            cursor.execute("INSERT INTO Telegram_messages(city_name,message,message_status) values(%s, %s, %s)",(city_name,new_url,message_status))
            mydb.commit()
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
    city4 = "Chennai 600096"
    city_url4 ='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode=600096&date='+date_edited
    telegram_url4 = 'https://api.telegram.org/bot1810454149:AAEnJXqIiBjvA7RwIlLQvuKnzpxrLAkn1dY/sendMessage?chat_id=-1001203466803&text='
    city5 = "Madurai"
    city_url5 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=540&date="+date_edited
    telegram_url5 = 'https://api.telegram.org/bot1699880333:AAHCpeud9uZUW_37Ph3WNPSoJE67Bj3JCM4/sendMessage?chat_id=-1001227954447&text='
    print("Trying for Chennai on", date_time_edited)
    get_vaccine(city1,city_url1,telegram_url1)
    print("Trying for Bengaluru-Urban on", date_time_edited)
    get_vaccine(city2,city_url2,telegram_url2)
    print("Trying for Bengaluru-BBMP on", date_time_edited)
    get_vaccine(city3,city_url3,telegram_url3)
    print("Trying for 600096")
    get_vaccine(city4,city_url4,telegram_url4)
    print("Trying for Madurai")
    get_vaccine(city5,city_url5,telegram_url5)
    time.sleep(10)
