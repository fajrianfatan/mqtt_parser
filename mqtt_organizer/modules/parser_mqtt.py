import paho.mqtt.client as mqtt
from parser_msg import create_parser_connector

# MQTT Broker settings
broker_address = "202.90.198.159"  # Replace with your MQTT broker's address
broker_port = 1883  # Default MQTT port
username = "bmkg_aws"
password = "bmkg_aws123"
topic = "#"  # Replace with the topic you want to subscribe to

# Create the parser_connector dictionary
parser_connector = create_parser_connector()
unparsed_database = {}

# Callback when the client connects to the broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(topic)

def on_message(client, userdata, message):
    if message.topic in parser_connector:
        # Parse the message using the corresponding parser function
        parsed = parser_connector[message.topic](message.payload.decode())
        print(parsed)
        # TODO: Insert parsed data into the database
        
    else:
        # Add to unparsed_database
        if message.topic in unparsed_database:
            unparsed_database[message.topic].append(message.payload.decode())
        else:
            unparsed_database[message.topic] = [message.payload.decode()]

# Create a client instance
client = mqtt.Client()

# Set the username and password
client.username_pw_set(username, password)

# Attach the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect(broker_address, broker_port, keepalive=60)

# Start the MQTT loop
client.loop_forever()
