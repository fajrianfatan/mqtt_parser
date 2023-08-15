import paho.mqtt.client as mqtt
import json

# MQTT Broker configuration
broker_address = '202.90.198.159'
broker_port = 1883
broker_username = 'bmkg_aws'
broker_password = 'bmkg_aws123'

# Global variables to store messages and the message count
message_count = 0
max_messages_per_file = 250

type_1_messages = []
type_2_messages = []
type_3_messages = []

# Function to parse date and time
def parse_date_time(date_str, time_str):
    # Assuming the date format is dd/mm/yyyy and time format is hh:mm:ss
    day, month, year = map(int, date_str.split('/'))
    hour, minute, second = map(int, time_str.split(':'))

    # Convert to the desired format "yyyy-mm-ddThh:mm:ssZ"
    formatted_time = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"
    return formatted_time

# Function to parse Type 1 message
def parse_message_type_1(payload):
    try:
        message = json.loads(payload)
        return message
    except json.JSONDecodeError as e:
        print(f"Error decoding Type 1 JSON: {e}")
        return None

# Function to parse Type 2 message
def parse_message_type_2(payload):
    try:
        message = json.loads(payload)
        data = message.get('data', [])
        parsed_messages = []
        for entry in data:
            formatted_time = parse_date_time(message.get('date'), entry.get('time'))
            parsed_entry = {
                'time': formatted_time,
                'rr': entry.get('rr'),
                'batt': message.get('bt'),
                'log_temp': entry.get('log_temp')
            }
            parsed_messages.append(parsed_entry)
        return parsed_messages
    except json.JSONDecodeError as e:
        print(f"Error decoding Type 2 JSON: {e}")
        return None

# Function to parse Type 3 message
def parse_message_type_3(payload):
    try:
        message = json.loads(payload)
        return {
            'time': message.get('time'),
            'rr': message.get('rr'),
            'batt': message.get('bt'),
            'log_temp': message.get('log_temp')
        }
    except json.JSONDecodeError as e:
        print(f"Error decoding Type 3 JSON: {e}")
        return None

# Function to export messages to JSON file
def export_to_json(file_name, messages):
    try:
        with open(file_name, 'w') as file:
            json.dump(messages, file)
        print(f"Exported to {file_name}")
    except Exception as e:
        print(f"Error exporting to {file_name}: {e}")

# Callback when the client connects to the MQTT broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    # Subscribe to all topics
    client.subscribe('#')

# Callback when a message is received from the MQTT broker
def on_message(client, userdata, msg):
    global message_count, type_1_messages, type_2_messages, type_3_messages

    # Decode the message payload into a string using 'latin-1'
    try:
        payload = msg.payload.decode('latin-1')
    except UnicodeDecodeError:
        print(f"Error decoding message payload on topic {msg.topic} with 'latin-1' encoding.")
        return

    topic = msg.topic

    print(f"Received message on topic: {topic}")
    print(f"Raw payload: {payload}")

    # Increment the message count
    message_count += 1

    # Parse messages based on topic patterns
    if topic.startswith('device/sumsel/arg/'):
        parsed_message = parse_message_type_1(payload)
        if parsed_message:
            type_1_messages.append(parsed_message)
    elif 'cr6' in topic or 'cr1000x' in topic:
        parsed_messages = parse_message_type_2(payload)
        if parsed_messages:
            type_2_messages.extend(parsed_messages)
    elif topic.startswith('device/jatim/arg/'):
        parsed_message = parse_message_type_3(payload)
        if parsed_message:
            type_3_messages.append(parsed_message)

    # Export messages to JSON files when the count reaches 250
    if message_count == max_messages_per_file:
        if type_1_messages:
            export_to_json('type1_messages.json', type_1_messages)
            type_1_messages.clear()
        if type_2_messages:
            export_to_json('type2_messages.json', type_2_messages)
            type_2_messages.clear()
        if type_3_messages:
            export_to_json('type3_messages.json', type_3_messages)
            type_3_messages.clear()

        # Reset the message count
        message_count = 0

# Create an MQTT client instance
client = mqtt.Client()

# Set the username and password for the broker if required
client.username_pw_set(broker_username, broker_password)

# Assign callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker_address, broker_port)

# Start the MQTT loop in a non-blocking way
client.loop_start()

# Keep the script
#v2