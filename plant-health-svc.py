import time
import paho.mqtt.client as paho
import mysql.connector

import config

mydb = mysql.connector.connect(
  host=config.mysql_host,
  user=config.mysql_user,
  password=config.mysql_password
)

print(mydb)

mycursor = mydb.cursor()

#define callback
def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("moisture")

def on_message(client, userdata, message):
    #time.sleep(1)
    msg = str(message.payload.decode("utf-8"))
    print("received message =", msg)

    sql = "INSERT INTO `plant-health-db`.moisture (sensor_name, obs_time, val) VALUES (%s, %s, %s)"
    val = ("sensor_1", time.strftime('%Y-%m-%d %H:%M:%S'), msg)
    mycursor.execute(sql, val)

    mydb.commit()
    print(mycursor.rowcount, "record inserted.")

client= paho.Client("client-001")
client.connect(config.mqtt_host)
client.on_connect = on_connect
client.on_message=on_message

client.loop_forever()






