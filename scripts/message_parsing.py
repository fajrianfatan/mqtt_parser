import paho.mqtt.client as mqtt
import json

# MQTT Broker configuration
broker_address = '202.90.198.159'
broker_port = 1883
broker_username = 'bmkg_aws'
broker_password = 'bmkg_aws123'


def parse_message_type_1(payload):
    try:
        # Parse the original payload
        date_str = payload['date']
        time_str = payload['time']
        rr = payload['rr']
        bt = payload['batt']  # Change 'batt' to 'bt'
        log_temp = payload['log_temp']
        id = payload['id']
        site = payload['site']

        # Combine date and time into 'time'
        formatted_time = f"{date_str}T{time_str}Z"

        # Create the converted payload
        converted_payload = {
            'time': formatted_time,
            'rr': rr,
            'bt': bt,  # Use 'bt' instead of 'batt'
            'log_temp': log_temp,
            'id': id,
            'site': site
        }

        return converted_payload
    except json.JSONDecodeError as e:
        print(f"Error decoding Type 1 JSON: {e}")
        return None

def parse_message_type_2(payload):
    try:
        # Parse the original payload
        date_str = payload['date']
        time_str = payload['time']
        rr = payload['rr']
        bt = payload['batt']  # Change 'batt' to 'bt'
        log_temp = payload['log_temp']
        id = payload['id']
        site = payload['site']

        # Combine date and time into 'time'
        formatted_time = f"{date_str}T{time_str}Z"

        # Create the converted payload
        converted_payload = {
            'time': formatted_time,
            'rr': rr,
            'bt': bt,  # Use 'bt' instead of 'batt'
            'log_temp': log_temp,
            'id': id,
            'site': site
        }

        return converted_payload
    except json.JSONDecodeError as e:
        print(f"Error decoding Type 2 JSON: {e}")
        return None

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
def parse_message_type_4(payload):
    try:
        # Function to parse date and time to RFC 3339 format
        def parse_date_time(date_str, time_str):
            day, month, year = map(int, date_str.split('/'))
            hour, minute, second = map(int, time_str.split(':'))

            formatted_time = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"
            return formatted_time

        # Function to parse a single message
        def parse_single_message(topic, payload):
            parsed_message = {}

            if topic.endswith('/time'):
                # Parse 'time' to RFC 3339 format
                date_str, time_str = payload.split()
                parsed_message['time'] = parse_date_time(date_str, time_str)

            elif topic.endswith('/bt'):
                # Convert 'batt' key name to 'bt'
                parsed_message['bt'] = float(payload)

            else:
                # Parse the rest of the payload
                try:
                    key = topic.split('/')[-1]
                    parsed_message[key] = float(payload)
                except ValueError:
                    print(f"Error parsing payload for topic: {topic}")

            return parsed_message

        # Store messages for each device
        device_messages = {}

        # Function to process messages for a device
        def process_device_messages(device_id):
            if device_id in device_messages:
                parsed_payload = {}
                for topic, payload in device_messages[device_id]:
                    parsed_message = parse_single_message(topic, payload)
                    parsed_payload.update(parsed_message)

        # Now you have the complete payload for the device
        print(json.dumps(parsed_payload))

        # Clear messages for this device
        del device_messages[device_id]

    # Sample data for testing (received one by one)
        messages = [
            message from payload
        ]

        # Process the messages
        for topic, payload in messages:
            device_id = topic.split('/')[-2]
            if device_id not in device_messages:
                device_messages[device_id] = []

            device_messages[device_id].append((topic, payload))

            # Check if this is the last message for the device
            if topic.endswith('/slr_max'):
                process_device_messages(device_id)

    except json.JSONDecodeError as e:
        print(f"Error decoding Type 3 JSON: {e}")
        return None
    
def parse_message_type_5(payload):
    try:
        # Parse the original payload
        date_str = payload['date']
        time_str = payload['time']
        rr = payload['rr']
        bt = payload['batt']  # Change 'batt' to 'bt'
        log_temp = payload['log_temp']
        id = payload['id']
        site = payload['site']
        shunt = payload['shunt']
        curr = payload['curr'] 

        # Combine date and time into 'time'
        formatted_time = f"{date_str}T{time_str}Z"

        # Create the converted payload
        converted_payload = {
            'time': formatted_time,
            'rr': rr,
            'bt': bt,  # Use 'bt' instead of 'batt'
            'log_temp': log_temp,
            'id': id,
            'site': site,
            'shunt': shunt,
            'curr': curr
        }

        return converted_payload
    except json.JSONDecodeError as e:
        print(f"Error decoding Type 3 JSON: {e}")
        return None
# Callback when the client connects to the MQTT broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    # Subscribe to all topics
    client.subscribe('#')

# Callback when a message is received from the MQTT broker
def on_message(client, userdata, msg):
    # Decode the message payload into a string using 'latin-1'
    try:
        payload = msg.payload.decode('latin-1')
    except UnicodeDecodeError:
        print(f"Error decoding message payload on topic {msg.topic} with 'latin-1' encoding.")
        return

    topic = msg.topic

    print(f"Received message on topic: {topic}")
    print(f"Raw payload: {payload}")

    # Parse messages based on topic patterns
    if topic.startswith('device/sumsel/arg/'):
        parsed_message = parse_message_type_1(payload)
    elif 'cr6' in topic or 'cr1000x' in topic:
        parsed_message = parse_message_type_2(payload)
    elif topic.startswith('device/jatim/arg/'):
        parsed_message = parse_message_type_3(payload)
    else:
        # Add more parsing functions for other message types if needed
        parsed_message = None

    if parsed_message:
        print(f"Parsed message from topic {topic}:")
        print(parsed_message)

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

# Keep the script running to receive messages
try:
    while True:
        pass
except KeyboardInterrupt:
    # Disconnect gracefully on keyboard interrupt (Ctrl+C)
    client.disconnect()
    client.loop_stop()
