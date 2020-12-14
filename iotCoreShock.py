from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time as t
import json
import serial

# Define ENDPOINT, CLIENT_ID, PATH_TO_CERT, PATH_TO_KEY, PATH_TO_ROOT, MESSAGE, TOPIC, and RANGE
ENDPOINT = "a1jpskevl8sr71-ats.iot.us-east-1.amazonaws.com"
CLIENT_ID = "testDevice"
PATH_TO_CERT = "certificates/certificate.pem.crt"
PATH_TO_KEY = "certificates/private.pem.key"
PATH_TO_ROOT = "certificates/root.pem"
# MESSAGE = "Hello World"
TOPIC = "test/topic"
TOPIC2 = "test/test"
RANGE = 20

# Spin up resources
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERT,
            pri_key_filepath=PATH_TO_KEY,
            client_bootstrap=client_bootstrap,
            ca_filepath=PATH_TO_ROOT,
            client_id=CLIENT_ID,
            clean_session=False,
            keep_alive_secs=6
            )


def on_message_received(topic, payload, **kwargs):
    print("Received message from topic '{}': {}".format(topic, payload))

print("Connecting to {} with client ID '{}'...".format(
        ENDPOINT, CLIENT_ID))
# Make the connect() call
connect_future = mqtt_connection.connect()
# Future.result() waits until a result is available
connect_future.result()
print("Connected!")
# Publish message to server desired number of times.
print('Begin Publish')

# for i in range(RANGE):
while True:
    ser = serial.Serial('/dev/ttyACM0', 9600, timeout=1)
    ser.flush()
    if ser.in_waiting > 0:
        line = ser.readline().decode('utf-8').rstrip()

        if line == "":
           line = 0
        data = "{}".format(line)
        line = {"shock" : data}
        
        mqtt_connection.publish(topic=TOPIC, payload=json.dumps(line), qos=mqtt.QoS.AT_LEAST_ONCE)
        # mqtt_connection.subscribe(topic=TOPIC2, qos=mqtt.QoS.AT_LEAST_ONCE)
        print("Published: '" + json.dumps(line) + "' to the topic: " + "'test/testing'")
        subscribe_future, packet_id = mqtt_connection.subscribe(
                topic=TOPIC,
                qos=mqtt.QoS.AT_LEAST_ONCE,
                callback=on_message_received)
        t.sleep(0.1)

print('Publish End')

# Subscribe
print("Subscribing to topic '{}'...".format(TOPIC))
subscribe_result = subscribe_future.result()
print("Subscribed with {}".format(str(subscribe_result['qos'])))
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()