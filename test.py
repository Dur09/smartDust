import paho.mqtt.publish as publish
import paho.mqtt.client as mqtt
import sqlite3
import time
import json
import Queue
import threading
import sys
from itertools import izip

dustbin = sqlite3.connect('/site/smartDust/db/dust.db')
print "Opened database successfully";

def is_json(myjson):
    try:
        json_object = json.loads(myjson)
    except ValueError, e:
        print 'not a json'
        return False
    return True

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("/iot/dev/00001")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global accessConn
    print(str(msg.payload))

    if is_json(msg.payload):
        deviceData = msg.payload
        epoch = str(int(time.time()))
        query = "INSERT INTO dust_data ('payload','epoch') values ('"+deviceData+"','"+epoch+"')"
        print query
        cursor = dustbin.execute(query)
        dustbin.commit()

try:
        client = mqtt.Client()
        #client.username_pw_set("00001", "@smartBin123")
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect("test.mosquitto.org", 1883, 60)
        client.loop_forever()
        print "started mains"
except(KeyboardInterrupt, SystemExit):
    runThread = False
    #cleanup_stop_thread()
    sys.exit()

# This part will be used when retrieving data using API to render webpage
'''
sample = '[{"id":"2","loc":"KRV","ts":"1541315096","state":"FULL"}]'
json_obj = json.loads(sample);
location = json_obj[0]
json_obj = location['loc']
print json_obj

with open('locMapper.json') as f:
    data = json.load(f)
    for i, val in enumerate(data):
        if json_obj in data[i]:
            print "Exists" 
            break
        else:
           print "not found"
'''
