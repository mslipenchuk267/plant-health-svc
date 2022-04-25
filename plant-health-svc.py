import time
import paho.mqtt.client as paho
import mysql.connector

import config

broker=config.mqtt_host

#define callback
def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("moisture")

def on_message(client, userdata, message):
    #time.sleep(1)
    print("received message =", str(message.payload.decode("utf-8")))

client= paho.Client("client-001")
client.connect(broker)
client.on_connect = on_connect
client.on_message=on_message

client.loop_forever()


#mydb = mysql.connector.connect(
#  host=config.mysql_host,
#  user=config.mysql_user,
#  password=config.mysql_password
#)
#
#print(mydb)
#
#mycursor = mydb.cursor()
#
#sql = "INSERT INTO `plant-health-db`.moisture (sensor_name, obs_time, val) VALUES (%s, %s, %s)"
#val = ("sensor_1", time.strftime('%Y-%m-%d %H:%M:%S'), 99)
#
#mycursor.execute(sql, val)
#
#mydb.commit()
#
#print(mycursor.rowcount, "record inserted.")