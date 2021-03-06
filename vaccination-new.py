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


class time_control_process():
    def __init__(self, working_period, sleeping_period):
        self.working_period = working_period
        self.sleeping_period = sleeping_period
        self.reset()

    def reset(self):
        self.start_time = datetime.utcnow()

    def manage(self):
        m = (datetime.utcnow() - self.start_time).seconds / 60
        if m >= self.working_period:
            time.sleep(self.sleeping_period * 60)
            self.reset()
        return

tcp = time_control_process(60, 60)

def get_vaccine(age,city_name,city_url,telegram_url):
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
    queried_result = normalized_table_sessions.query('available_capacity>0 & min_age_limit == @age & available_capacity_dose1>0')
    if queried_result.empty:
        print("Response: No vaccine slots open")
        message = "Response: No vaccine slots open"
        message_status = "Not sent"
        cursor.execute("INSERT INTO Telegram_messages(city_name,message,message_status) values(%s, %s, %s)",(city_name,message,message_status))
        mydb.commit()
        new_url = message
    else:
        vaccination_centers = queried_result.loc[:,['date','name','block_name','pincode','vaccine','available_capacity','available_capacity_dose1','available_capacity_dose2']].drop_duplicates()
        #table = vaccination_centers.to_string(columns = ['date','name','pincode','vaccine','available_capacity'], index = False, header = False, line_width = 70, justify = 'left')
        table =''
        for i in range(len(vaccination_centers)):
            table = table + '\nDate: ' + str((vaccination_centers['date'].iloc[i]))+'\nHospital: '+'<b>'+str((vaccination_centers['name'].iloc[i]))+'</b>'+'('+str((vaccination_centers['block_name'].iloc[i]))+')'+'\nPincode: '+'<b>'+str((vaccination_centers['pincode'].iloc[i]))+'</b>'+'\nVaccine: '+'<b>'+str((vaccination_centers['vaccine'].iloc[i]))+'</b>'+'\nTotal Available slots: '+'<b>'+str((vaccination_centers['available_capacity'].iloc[i]))+' slots</b>'+'\nDose 1 Slots Available: '+'<b>'+str((vaccination_centers['available_capacity_dose1'].iloc[i])) +' slots</b>'+'\nDose 2 Slots Available: '+'<b>'+ str((vaccination_centers['available_capacity_dose2'].iloc[i])) +' slots\n</b>'
        new_url = telegram_url+'<b>[18-44 years] [First dose slots available]\n</b>'+table+"\n You can book this slot at https://selfregistration.cowin.gov.in/"+"&parse_mode=html"
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
            try:
                message_status = "Sent"
                cursor.execute("INSERT INTO Telegram_messages(city_name,message,message_status) values(%s, %s, %s)",(city_name,new_url,message_status))
                mydb.commit()
                requests.get(new_url)
            except Exception as e:
                print(e)


while True:
    tcp.manage()
    try:
        today = date.today()
        now = datetime.now()
        now_hour = datetime.now().hour
        date_edited = today.strftime("%d-%m-%Y")
        date_time_edited = now.strftime("%d-%m-%Y %H:%M:%S")
        age = 18
        age_elders = 45
        city1 = "Chennai"
        city_url1 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=571&date="+date_edited
        telegram_url1 = 'https://api.telegram.org/bot1761680572:AAGibV1_ZI_F92mtr9pQqH-rXF58V-7Xkf4/sendMessage?chat_id=-1001335725303&text='
        city2 = "Bengaluru Urban"
        city_url2 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=265&date="+date_edited
        telegram_url2 = 'https://api.telegram.org/bot1760779664:AAHoHoy6YUmQB9WXuID4dquwb9WWWGNzP7Q/sendMessage?chat_id=-1001383423441&text='
        city3 = "BBMP"
        city_url3 = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=294&date="+date_edited
        telegram_url3 = 'https://api.telegram.org/bot1760779664:AAHoHoy6YUmQB9WXuID4dquwb9WWWGNzP7Q/sendMessage?chat_id=-1001383423441&text='
        city4 = "Chennai 600096"
        city_url4 ='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode=600096&date='+date_edited
        telegram_url4 = 'https://api.telegram.org/bot1810454149:AAEnJXqIiBjvA7RwIlLQvuKnzpxrLAkn1dY/sendMessage?chat_id=-1001203466803&text='
        city5 = "Madurai"
        city_url5 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=540&date="+date_edited
        telegram_url5 = 'https://api.telegram.org/bot1699880333:AAHCpeud9uZUW_37Ph3WNPSoJE67Bj3JCM4/sendMessage?chat_id=-1001227954447&text='
        city6 = "Salem"
        city_url6 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=545&date="+date_edited
        telegram_url6 = 'https://api.telegram.org/bot1823073510:AAEbbsWW2aVB6jQMJtqQQxA4W0RigNnnnPM/sendMessage?chat_id=-1001404134647&text='
        telegram_url7 = 'https://api.telegram.org/bot1702698090:AAHcS15pS6OEHVGhQrpJGqGgVwn-4_ct8bk/sendMessage?chat_id=-1001380816650&text='
        city8 = "Hyderabad"
        city_url8 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=581&date="+date_edited
        telegram_url8 = 'https://api.telegram.org/bot1807501550:AAHQJdjoXs4DCA979cm7p3mUQIKugTARNCc/sendMessage?chat_id=-1001267490494&text='
        city9 = "Mumbai"
        city_url9 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=395&date="+date_edited
        telegram_url9 = 'https://api.telegram.org/bot1803859695:AAGFsDfHsaX2WCIS2Fjfrh_886FI0UQdR4U/sendMessage?chat_id=-1001289913000&text='
        city10 = "Gurgaon"
        city_url10 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=188&date="+date_edited
        telegram_url10 = 'https://api.telegram.org/bot1881126551:AAGp57bhqtxd1gkOKR_FS4nhK1jt5XsV19U/sendMessage?chat_id=-1001305001942&text='
        city11 = "Aurangabad,Bihar"
        city_url11 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=77&date="+date_edited
        telegram_url11 = 'https://api.telegram.org/bot1467732009:AAEvNhb9V23ZiWhU_-qbf4Zec7zbqe8SdAk/sendMessage?chat_id=-1001294351334&text='
        city12 = "Kolkata"
        city_url12 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=725&date="+date_edited
        telegram_url12 = 'https://api.telegram.org/bot1759259530:AAFRkNH8m51K6dp3uQ5Z669aKaesaOLtAS4/sendMessage?chat_id=-1001332879830&text='
        city13 = "Coimbatore"
        city_url13 ="https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id=539&date="+date_edited
        telegram_url13 = 'https://api.telegram.org/bot1875276934:AAFoxwZWU_wTFBcRfFr2dBiZxeYbynnSypw/sendMessage?chat_id=-1001300304878&text='
        #Instance1
        print("Trying for Chennai on", date_time_edited)
        get_vaccine(age,city1,city_url1,telegram_url1)
        chennai_pincode=[600026,600028,600030,600006,600096,600087,600015,600091,600061,600042,600020,600085,600081,600100,600102,600042,600020,600018,600035,600010]
        for i in chennai_pincode:
            print("Trying for",i)
            city_name='From '+ str(i)
            city_url_loop='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode='+ str(i) +'&date='+date_edited
            get_vaccine(age,city_name,city_url_loop,telegram_url1)
        print("Trying for Madurai")
        get_vaccine(age,city5,city_url5,telegram_url5)
        print("Trying for Salem")
        get_vaccine(age,city6,city_url6,telegram_url6)
        #Instance3
        print("Trying for Bengaluru-Urban on", date_time_edited)
        get_vaccine(age,city2,city_url2,telegram_url2)
        print("Trying for Bengaluru-BBMP on", date_time_edited)
        get_vaccine(age,city3,city_url3,telegram_url3)
        bengaluru_pincode=[560038,560008,560071,560003,560079,560102,560034,560020,560100,560099,560036,560023,560029,562106,560007,562157,560076,560098,560099,560086,560054,560001,560045,560010,560047,560103,560035,560011]
        for i in bengaluru_pincode:
            print("Trying for ",i)
            city_name='From '+ str(i)
            city_url_loop='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode='+ str(i) +'&date='+date_edited
            get_vaccine(age,city_name,city_url_loop,telegram_url2)
        print("Trying for Gurgaon")
        get_vaccine(age,city10,city_url10,telegram_url10)
        print("Trying for Mumbai")
        get_vaccine(age,city9,city_url9,telegram_url9)
        #Instance5
        print("Trying for Hyderabad")
        get_vaccine(age,city8,city_url8,telegram_url8)
        print("Trying for Aurangabad,Bihar")
        get_vaccine(age,city11,city_url11,telegram_url11)
        aurangabad_pincode=[824125,824203,824143,824101,824208,824120,824203,824202,824124,824301,824208,824123]
        for i in aurangabad_pincode:
            print("Trying for ",i)
            city_name='From '+ str(i)
            city_url_loop='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode='+ str(i) +'&date='+date_edited
            get_vaccine(age,city_name,city_url_loop,telegram_url11)
        hyderabad_pincode=[500009,500029,500003,500033,500004,500034,500038]
        for i in hyderabad_pincode:
            print("Trying for ",i)
            city_name='From '+ str(i)
            city_url_loop='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode='+ str(i) +'&date='+date_edited
            get_vaccine(age,city_name,city_url_loop,telegram_url8)
        #Instance7
        mumbai_pincode=[400064,400012,400004,400026,400010,400004,400005,400020,400050,400071,400056,400034,400052,400053,400002,400088,400016]
        for i in mumbai_pincode:
            print("Trying for ",i)
            city_name='From '+ str(i)
            city_url_loop='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode='+ str(i) +'&date='+date_edited
            get_vaccine(age,city_name,city_url_loop,telegram_url9)
        print("Trying for Kolkata")
        get_vaccine(age,city12,city_url12,telegram_url12)
        kolkata_pincode=[700019,700054,700092,700025,700020,711104,700007,700010,711101,700027,700012]
        for i in kolkata_pincode:
            print("Trying for ",i)
            city_name='From '+ str(i)
            city_url_loop='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode='+ str(i) +'&date='+date_edited
            get_vaccine(age,city_name,city_url_loop,telegram_url12)
        gurgaon_pincode=[122001,122506,122017,122002,122051,122103,122004,122503]
        for i in gurgaon_pincode:
            print("Trying for ",i)
            city_name='From '+ str(i)
            city_url_loop='https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByPin?pincode='+ str(i) +'&date='+date_edited
            get_vaccine(age,city_name,city_url_loop,telegram_url10)
        print("Trying for Coimbatore")
        get_vaccine(age,city13,city_url13,telegram_url13)
        time.sleep(10)
        print("End of Try block")
    except Exception as e:
        print(e)
        print("Restarting the code from local")
        time.sleep(10)
        continue
    else:
        continue
