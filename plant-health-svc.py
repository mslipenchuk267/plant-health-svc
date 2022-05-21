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

# define on connect event handler
def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("#")

# define messaged received event handler
def on_message(client, userdata, message):
    obs_time = datetime.strptime(str(datetime.now()), '%Y-%m-%d %H:%M:%S.%f')
    msg = str(message.payload.decode("utf-8"))
    print("received message =", msg)

    # Parse Topic
    msg_topic = message.topic
    topic_device_id = msg_topic.split("/")[0]
    topic_sensor = msg_topic.split("/")[1]

    if (topic_sensor == "moisture"):
      split_msg = msg.split(",")
      sensor_name = split_msg[0]
      battery_v = split_msg[1]
      soil_v = split_msg[2]
      atm_v = split_msg[3]
      soil_v_count = split_msg[4]
      atm_v_count = split_msg[5]
      soil_moisture_percent = split_msg[6]
      atm_moisture_percent = split_msg[7]
      rel_moisture_percent = split_msg[8]

      data = {}
      data['sensor_name'] = split_msg[0]
      data['battery_v'] = split_msg[1]
      data['soil_v'] = split_msg[2]
      data['atm_v'] = split_msg[3]
      data['soil_v_count'] = split_msg[4]
      data['atm_v_count'] = split_msg[5]
      data['soil_moisture_percent'] = split_msg[6]
      data['atm_moisture_percent'] = split_msg[7]
      data['rel_moisture_percent'] = split_msg[8]

      sql = "INSERT INTO `plant-health-db`.moisture (sensor_name, obs_time, battery_v, soil_v, atm_v, soil_v_count, atm_v_count, soil_moisture_percent, atm_moisture_percent, rel_moisture_percent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
      val = (sensor_name, obs_time, battery_v, soil_v, atm_v, soil_v_count, atm_v_count, soil_moisture_percent, atm_moisture_percent, rel_moisture_percent)
    
    mycursor.execute(sql, val)
    mydb.commit()
    print(mycursor.rowcount, "record inserted.")

def main():
  try:
    client= paho.Client("mysql_proc")
    client.on_connect = on_connect
    client.on_message=on_message
    print(config.mqtt_host)
    res = client.connect(config.mqtt_host, 1883, 60)
    client.loop_forever()
  except Exception as e:
        return main()

main()
