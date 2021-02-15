#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
from datetime import date
import datetime
import calendar
import psycopg2

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer = Consumer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'group.id': 'CTran_Sensor_Readings',
        'auto.offset.reset': 'earliest',
    })

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    trip_ids=[]
    conn = psycopg2.connect(database="redhots",user="postgres",password="postgres",host="127.0.0.1")
    conn.autocommit = True
    cursor = conn.cursor();
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                invalid1=invalid2=invalid3=invalid4=invalid5=invalid6=invalid7=invalid8=invalid9=invalid10=0
                isValidDate=True
                #print(data)
                for k,v in data.items():
                    if k=="EVENT_NO_TRIP" and v=="":
                        invalid1=1
                    if k=="VEHICLE_ID" and v=="":
                        invalid2=1
                    if k=="DIRECTION":
                        if v=="":
                            invalid2=0
                        elif int(v) not in range(0,360) :
                            invalid3=1
                    """if k=="EVENT_NO_TRIP" and v!="":
                        if data["VEHICLE_ID"]=="":
                            invalid4=1"""
                    if k=="GPS_LATITUDE":
                        if v=="":
                            invalid4=1
                        elif float(v)>45.766234 or float(v)<45.480231:
                            invalid4=1
                    if k=="GPS_LONGITUDE":
                        if v=="":
                            invalid5=1
                        elif abs(float(v))<(122.350367) or abs(float(v))>(122.743269):
                            invalid5=1
                    if k=="OPT_DATE" and v!="":
                        if data["ACT_TIME"]=="":
                            invalid6=1
                    elif k=="OPT_DATE" and v=="":
                        invalid6=1

                    if k=="OPD_DATE":
                        f="%d-%b-%y"
                        #f1="%Y-%m-%d"
                        try:
                            datetime.datetime.strptime(v,f)
                        except ValueError:
                            isValidDate = False
                        if(isValidDate):
                            invalid7=0
                        else:
                            invalid7=1
                    if k=="ACT_TIME":
                        if v=="":
                            invalid8=1
                        elif int(v) < 15040 or int(v) > 90712:
                            invalid8=1
                    if k=="VEHICLE_ID":
                        if len(v) != 4:
                            invalid9=1
                    if k=="EVENT_NO_TRIP" and v!="":
                        if data["EVENT_NO_STOP"]=="":
                            invalid10=1
                    elif k=="EVENT_NO_TRIP" and v=="":
                        invalid10=1
                #dt= date.today()
                #today=dt.strftime("%Y-%m-%d")
                #filename="Consumer/"+today+".json"
                #filename="Consumer/15.json"
                if(invalid1==0 and invalid2==0 and invalid3 ==0 and invalid4==0 and invalid5==0 and invalid6==0 and invalid7==0 and invalid8==0 and invalid9==0 and invalid10==0):
                    date=data["OPD_DATE"]
                    time=data["ACT_TIME"]
                    f1 = datetime.datetime.strptime(date,f)
                    newdate=f1.strftime("%Y-%m-%d")
                    if int(time) > 86399:
                        time  = int(time) - 86400
                    newtime=datetime.timedelta(seconds=int(time))
                    dt=str(newdate)+" "+str(newtime)
                    tstamp = datetime.datetime.strptime(dt,"%Y-%m-%d %H:%M:%S")
                    day = datetime.datetime.strptime(newdate,"%Y-%m-%d").weekday()
                    l = ["Monday","Tuesday","Wednesday","Thursday","Friday"]
                    d = calendar.day_name[day]
                    #print(type(d))
                    if d in l:
                        service_key = "Weekday"
                    else:
                        service_key = d
                    if(data["VELOCITY"]!=""):
                        speed=float(data["VELOCITY"])*2.2369
                    else:
                        speed=None

                    #print(speed)
                    vehicle =int( data["VEHICLE_ID"])
                    
                    sql_trip = """insert into trip(trip_id, route_id, vehicle_id, service_key,direction) values(%s,%s,%s,%s,%s)""" 
                    if data["EVENT_NO_TRIP"] not in trip_ids:
                        trip_Temp = int(data["EVENT_NO_TRIP"])
                        trip_ids.append(data["EVENT_NO_TRIP"])
                        route=direc=None
                        cursor.execute(sql_trip, (trip_Temp,route,vehicle,service_key,direc))
                    #print(trip_ids)
                    latitude=float(data["GPS_LATITUDE"])
                    
                    longitude=float(data["GPS_LONGITUDE"])
                    if data["DIRECTION"]!="":
                        direction=int(data["DIRECTION"])
                    else:
                        direction=None
                    trip=int(data["EVENT_NO_TRIP"])

                    sql = """INSERT INTO breadcrumb(tstamp, latitude, longitude, direction, speed, trip_id) VALUES (%s,%s,%s,%s,%s,%s)"""
                    cursor.execute(sql, (tstamp,latitude,longitude,direction,speed,trip))
                #else:
                    #print("Failed")
                    #print(invalid1,invalid2,invalid3,invalid4,invalid5,invalid6,invalid7,invalid8,invalid9,invalid10)
                    
                """with open(filename, "a") as fn:
                    json.dump(data, fn)
                total_count += 1
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value,total_count))"""
        
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
