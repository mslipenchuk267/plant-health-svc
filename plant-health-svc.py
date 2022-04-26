import os
import time
import json
import paho.mqtt.client as paho
from datetime import datetime
from azure.iot.device import IoTHubDeviceClient
from azure.core.exceptions import AzureError
from azure.storage.blob import BlobClient

import config
cwd = os.getcwd()

device_client = IoTHubDeviceClient.create_from_connection_string(config.adls_connection_str)
device_client.connect()

#define callback
def store_blob(blob_info, file_name, file_contents):
    try:
        sas_url = "https://{}/{}/{}{}".format(
            blob_info["hostName"],
            blob_info["containerName"],
            blob_info["blobName"],
            blob_info["sasToken"]
        )

        print("\nUploading file: {} to Azure Storage as blob: {} in container {}\n".format(file_name, blob_info["blobName"], blob_info["containerName"]))

        # Upload the specified file
        with BlobClient.from_blob_url(sas_url) as blob_client:
            result = blob_client.upload_blob(file_contents, overwrite=True)
            return (True, result)

    except FileNotFoundError as ex:
        # catch file not found and add an HTTP status code to return in notification to IoT Hub
        ex.status_code = 404
        return (False, ex)

    except AzureError as ex:
        # catch Azure errors that might result from the upload operation
        return (False, ex)

def on_connect(client, userdata, flags, rc):
  print("Connected with result code "+str(rc))
  client.subscribe("moisture")

def on_message(client, userdata, message):
    obs_time = datetime.strptime(str(datetime.now().astimezone()), '%Y-%m-%d %H:%M:%S.%f%z')

    msg = str(message.payload.decode("utf-8"))
    print("received message =", msg)

    split_msg = msg.split(",")
    data = {}
    data['sensor_name'] = split_msg[0]
    data['obs_time'] = str(obs_time) + " "
    data['soil_v'] = split_msg[1]
    data['atm_v'] = split_msg[2]
    data['soil_v_count'] = split_msg[3]
    data['atm_v_count'] = split_msg[4]
    data['soil_moisture_percent'] = split_msg[5]
    data['atm_moisture_percent'] = split_msg[6]
    data['rel_moisture_percent'] = split_msg[7]
    json_data = json.dumps(data)

    timestr = time.strftime("%Y%m%d-%H%M%S")
    file_name = "moisture_" + timestr + ".json"
    file_path = os.path.join(cwd, file_name)

    blob_name = file_name
    storage_info = device_client.get_storage_info_for_blob(blob_name)
    # Upload to blob
    success, result = store_blob(storage_info, file_path, json_data)

    if success == True:
        print("Upload succeeded. Result is: \n") 
        print(result)
        print()

        device_client.notify_blob_upload_status(
            storage_info["correlationId"], True, 200, "OK: {}".format(file_path)
        )

    else :
        # If the upload was not successful, the result is the exception object
        print("Upload failed. Exception is: \n") 
        print(result)
        print()

        device_client.notify_blob_upload_status(
            storage_info["correlationId"], False, result.status_code, str(result)
        )

client= paho.Client("client-001")
client.connect(config.mqtt_host)
client.on_connect = on_connect
client.on_message=on_message

client.loop_forever()






