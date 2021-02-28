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
                    if k=="trip_id" and v=="":
                        invalid1=1            
                    if k=="vehicle_number" and v=="":
                        invalid2=1
                    if k=="direction":
                        if v=="":
                            invalid3=1
                        elif int(v) not in (0,1):
                            invalid3=1
                    if k=="vehicle_number":
                        if len(v) != 4:
                            invalid4=1
                    if k=="route_number" and v=="":
                        invalid5=1
                    if k=="service_key" and v=="":
                        invalid6=1
                    elif k=="service_key" and v!="":
                        if v not in ('W','U','S'):
                            invalid6=1

                if(invalid1==0 and invalid2==0 and invalid3 ==0 and invalid4==0 and invalid5==0 and invalid6==0):
                    if data["service_key"]=="W":
                        service_key = "Weekday"
                    elif data["service_key"]=="U":
                        service_key = "Sunday"
                    elif data["service_key"]=="S":
                        service_key = "Saturday"
                    route=int(data["route_number"])
                    trip_id=int(data["trip_id"])
                    vehicle =int( data["vehicle_number"])
                    d=int(data["direction"])
                    if d==1:
                        direction="Out"
                    elif d==0:
                        direction="Back"

                    cursor.execute("""select trip_id from trip2 where trip_id=%s""",[trip_id])
                    result=cursor.fetchall()
                    #print(result)
                    if (len(result)==0):
                        sql_trip = """insert into trip2(trip_id, route_id, vehicle_id, service_key,direction) values(%s,%s,%s,%s,%s)""" 
                        cursor.execute(sql_trip, (trip_id,route,vehicle,service_key,direction))
                    else:
                        sq="""update trip2 set route_id=%s, direction=%s where trip_id=%s"""
                        cursor.execute(sq, (route,direction,trip_id))
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
