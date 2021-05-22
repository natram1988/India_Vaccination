import requests
from pandas import json_normalize
import pandas as pd
import telebot
import requests
import time
from datetime import date,datetime

def get_vaccine(city_url,telegram_url):
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
    else:
        vaccination_centers = queried_result.loc[:,['date','name','pincode','vaccine','available_capacity']].drop_duplicates()
        table = vaccination_centers.to_string(columns = ['date','name','pincode','vaccine','available_capacity'], index = False, header = False, line_width = 70, justify = 'left')
        new_url = telegram_url+table
        print(new_url)
        requests.get(new_url)

while True:
        today = date.today()
        now = datetime.now()
        date_edited = today.strftime("%d-%m-%Y")
        date_time_edited = now.strftime("%d-%m-%Y %H:%M:%S")
        city_url1 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=571&date="+date_edited
        telegram_url1 = 'https://api.telegram.org/bot1761680572:AAHg3enK6v3JjMyaCaVr5UMqgmey11HN6WA/sendMessage?chat_id=-1001335725303&text='
        city_url2 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=294&date="+date_edited
        telegram_url2 = 'https://api.telegram.org/bot1760779664:AAE_SRx9Ca5t1ytiG8zwbKeqss_eLps6x4k/sendMessage?chat_id=-1001383423441&text='
        print("Trying for Chennai on", date_time_edited)
        get_vaccine(city_url1,telegram_url1)
        print("Trying for Bengaluru on", date_time_edited)
        get_vaccine(city_url2,telegram_url2)
        time.sleep(15)
