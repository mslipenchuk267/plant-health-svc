from datetime import datetime
import paho.mqtt.client as paho
import mysql.connector
import ctypes

import config

mydb = mysql.connector.connect(
  host=config.mysql_host,
  user=config.mysql_user,
  password=config.mysql_password
)

print(mydb)

mycursor = mydb.cursor()

class MoistureStruct(ctypes.Structure):
    _fields_ = (
        ('sensor_name', ctypes.c_char_p),
        ('soil_v', ctypes.c_double),
        ('atm_v', ctypes.c_double),
        ('soil_count_avg', ctypes.c_uint8),
        ('atm_count_avg', ctypes.c_uint8),
        ('soil_moisture_value', ctypes.c_uint8),
        ('atm_moisture_value', ctypes.c_uint8),
        ('relative_moisture_value', ctypes.c_uint8)
    )

#define callback
def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("moisture")

def on_message(client, userdata, message):
    obs_time = datetime.strptime(str(datetime.now()), '%Y-%m-%d %H:%M:%S.%f')

    msg = str(message.payload.decode("utf-8"))
    print("received message =", msg)

    split_msg = msg.split(",")
    sensor_name = split_msg[0]
    soil_v = split_msg[1]
    atm_v = split_msg[2]
    soil_v_count = split_msg[3]
    atm_v_count = split_msg[4]
    soil_moisture_percent = split_msg[5]
    atm_moisture_percent = split_msg[6]
    rel_moisture_percent = split_msg[7]

    data = {}
    data['sensor_name'] = split_msg[0]
    data['soil_v'] = split_msg[1]
    data['atm_v'] = split_msg[2]
    data['soil_v_count'] = split_msg[3]
    data['atm_v_count'] = split_msg[4]
    data['soil_moisture_percent'] = split_msg[5]
    data['atm_moisture_percent'] = split_msg[6]
    data['rel_moisture_percent'] = split_msg[7]

    sql = "INSERT INTO `plant-health-db`.moisture (sensor_name, obs_time, soil_v, atm_v, soil_v_count, atm_v_count, soil_moisture_percent, atm_moisture_percent, rel_moisture_percent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (sensor_name, obs_time, soil_v, atm_v, soil_v_count, atm_v_count, soil_moisture_percent, atm_moisture_percent, rel_moisture_percent)
    mycursor.execute(sql, val)

    mydb.commit()
    print(mycursor.rowcount, "record inserted.")

client= paho.Client("client-002")
client.on_connect = on_connect
client.on_message=on_message
print(config.mqtt_host)
res = client.connect(config.mqtt_host, 1883, 60)


client.loop_forever()






