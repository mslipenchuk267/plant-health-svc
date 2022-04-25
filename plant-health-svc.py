import time    
import mysql.connector

import config


mydb = mysql.connector.connect(
  host=config.host,
  user=config.user,
  password=config.password
)

print(mydb)

mycursor = mydb.cursor()

sql = "INSERT INTO `plant-health-db`.moisture (sensor_name, obs_time, val) VALUES (%s, %s, %s)"
val = ("sensor_1", time.strftime('%Y-%m-%d %H:%M:%S'), 99)

mycursor.execute(sql, val)

mydb.commit()

print(mycursor.rowcount, "record inserted.")