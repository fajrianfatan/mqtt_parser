import paho.mqtt.client as mqtt
import json

# MQTT Broker configuration
broker_address = '202.90.198.159'
broker_port = 1883
broker_username = 'bmkg_aws'
broker_password = 'bmkg_aws123'


# Function to parse date and time to RFC 3339 format
def parse_date_time(date_str, time_str):
    try:
        if not date_str or not time_str:
            return None

        date_parts = date_str.split('/')
        time_parts = time_str.split(':')

        if len(date_parts) != 3 or len(time_parts) != 3:
            return None

        day, month, year = map(int, date_parts)
        hour, minute, second = map(int, time_parts)

        formatted_time = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"
        return formatted_time
    except ValueError:
        return None

# Function to parse date and time to RFC 3339 format
def parse_date_time(date_str, time_str):
    day, month, year = map(int, date_str.split('/'))
    hour, minute, second = map(int, time_str.split(':'))

    formatted_time = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"
    return formatted_time

# Global variables to hold the current message data
current_message_time = None
current_message_data = {}

# Function to process and print the collected data
def process_collected_data():
    global current_message_time, current_message_data
    
    if current_message_time and current_message_data:
        current_message_data['time'] = current_message_time
        print("Processed collected data:")
        print(json.dumps(current_message_data))
    
    current_message_time = None
    current_message_data = {}

def parse_message_type_1(payload):
    try:
        if isinstance(payload, list) and len(payload) == 0:
            print("Payload is an empty list.")
            return None

        date_str = payload['date']
        time_str = payload['time']
        rr = payload['rr']
        bt = payload['batt']  # Change 'batt' to 'bt'
        log_temp = payload['log_temp']
        id = payload['id']
        site = payload['site']

        # Extract year, month, and day from the date string
        year, month, day = map(int, date_str.split('-'))
        
        # Extract hour, minute, and second from the time string
        hour, minute, second = map(int, time_str.split(':'))

        formatted_time = f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"
        converted_payload = {
            'time': formatted_time,
            'rr': rr,
            'bt': bt,  # Use 'bt' instead of 'batt'
            'log_temp': log_temp,
            'id': id,
            'site': site
        }

        return converted_payload
    except KeyError as e:
        print(f"KeyError while parsing Type 1 JSON: {e}")
        return None


# Function to parse message type 2
def parse_message_type_2(payload):
    try:
        time_str = payload['data'][0]['time']
        formatted_time = f"{time_str}Z"

        rr = payload['data'][0]['vals'][15]
        batt = payload['data'][0]['vals'][1]
        log_temp = payload['data'][0]['vals'][3]

        id = payload['head']['environment']['serial_no']
        site = payload['head']['environment']['station_name']

        parsed_payload = {
            'time': formatted_time,
            'rr': rr,
            'bt': batt,
            'log_temp': log_temp,
            'id': id,
            'site': site
        }

        return parsed_payload
    except KeyError as e:
        print(f"KeyError while parsing Type 2 JSON: {e}")
        return None

# Function to parse message type 3
def parse_message_type_3(payload):
    try:
        message = json.loads(payload)
        return {
            'time': message.get('time'),
            'rr': message.get('rr'),
            'bt': message.get('batt'),
            'log_temp': message.get('log_temp')
        }
    except json.JSONDecodeError as e:
        print(f"Error decoding Type 3 JSON: {e}")
        return None

# Function to parse message type 4
def parse_message_type_4(payload):
    try:
        def parse_single_message(topic, payload):
            parsed_message = {}

            if topic.endswith('/time'):
                date_str, time_str = payload.split()
                parsed_message['time'] = parse_date_time(date_str, time_str)
            elif topic.endswith('/bt'):
                parsed_message['bt'] = float(payload)
            else:
                try:
                    key = topic.split('/')[-1]
                    parsed_message[key] = float(payload)
                except ValueError:
                    print(f"Error parsing payload for topic: {topic}")

            return parsed_message

        device_messages = {}
        
        def process_device_messages(device_id):
            if device_id in device_messages:
                parsed_payload = {}
                for topic, payload in device_messages[device_id]:
                    parsed_message = parse_single_message(topic, payload)
                    parsed_payload.update(parsed_message)

                print(json.dumps(parsed_payload))
                del device_messages[device_id]

        messages = [
            # Add your real payload messages here
            ("device/dki/aws/STA2121/time", "26/03/2018 02:00:00"),
            ("device/dki/aws/STA2121/bt", "12.2"),
            ("device/dki/aws/STA2121/rr", "0"),
            ("device/dki/aws/STA2121/ws", "0.2"),
            ("device/dki/aws/STA2121/ws_max", "0.2"),
            ("device/dki/aws/STA2121/wd", "0"),
            ("device/dki/aws/STA2121/tt_max", "-78.28"),
            ("device/dki/aws/STA2121/tt", "-79.72"),
            ("device/dki/aws/STA2121/tt_min", "-81"),
            ("device/dki/aws/STA2121/rh", "0"),
            ("device/dki/aws/STA2121/pp", "497.1"),
            ("device/dki/aws/STA2121/slr", "0.008"),
            ("device/dki/aws/STA2121/slr_max", "0.008")
        ]

        for topic, payload in messages:
            device_id = topic.split('/')[-2]
            if device_id not in device_messages:
                device_messages[device_id] = []

            device_messages[device_id].append((topic, payload))

            if topic.endswith('/slr_max'):
                process_device_messages(device_id)

    except json.JSONDecodeError as e:
        print(f"Error decoding Type 4 JSON: {e}")
        return None

# Function to parse message type 5
def parse_message_type_5(payload):
    try:
        date_str = payload['date']
        time_str = payload['time']
        rr = payload['rr']
        bt = payload['batt']  # Change 'batt' to 'bt'
        log_temp = payload['log_temp']
        id = payload['id']
        site = payload['site']
        shunt = payload['shunt']
        curr = payload['curr'] 

        formatted_time = parse_date_time(date_str, time_str)

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
    except KeyError as e:
        print(f"KeyError while parsing Type 5 JSON: {e}")
        return None

def parse_message_type_6(topic, payload):
    global current_message_time, current_message_data

    try:
        if current_message_time is None:
            date_str, time_str = payload.split()
            formatted_time = parse_date_time(date_str, time_str)
            current_message_time = formatted_time
        else:
            key = topic.split('/')[-1]
            current_message_data[key] = payload

        process_collected_data()

    except ValueError as e:
        print(f"Error parsing payload for topic {topic}: {e}")

# Callback when the client connects to the MQTT broker
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    # Subscribe to all topics
    client.subscribe('#')

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode('latin-1')
    except UnicodeDecodeError:
        print(f"Error decoding message payload on topic {msg.topic} with 'latin-1' encoding.")
        return

    topic = msg.topic
    
    topic_parts = topic.split('/')
    location = topic_parts[1]
    
    # Skip messages with specific conditions
    if topic.endswith('/time') and payload.count('/') >= 2 and payload.count(':') >= 2:
        date_str, time_str = payload.split()
        formatted_time = parse_date_time(date_str, time_str)
        parsed_message = {
            'time': formatted_time
        }
        print(f"Parsed Type 6 message from topic {topic}:")
        print(parsed_message)
        return  # Skip processing further
    # Skip messages with specific clientId
    try:
        message = json.loads(payload)
        if 'clientId' in message and message['clientId']:
            print("Excluded message with clientId. Skipping processing.")
            return
    except json.JSONDecodeError:
        pass  
    # Add more conditions to skip messages as needed
    # For example:
    # if topic.startswith('device/excluded'):
    #     print("Excluded topic. Skipping processing.")
    #     return

    print(f"Received message on topic: {topic}")
    print(f"Raw payload: {payload}")

    # Parse messages based on location and topic patterns
    if location == 'jabar' and topic_parts[2] == 'arg':
        try:
            parsed_message = parse_message_type_1(json.loads(payload))
        except (json.JSONDecodeError, TypeError):
            print("Payload is not in JSON format for Type 1 message.")
            return
    elif 'cr6' in topic or 'cr1000x' in topic:
        try:
            parsed_message = parse_message_type_2(json.loads(payload))
        except (json.JSONDecodeError, TypeError):
            print("Payload is not in JSON format for Type 2 message.")
            return
    elif location == 'jabar' and topic_parts[2] == 'arg':
        parsed_message = parse_message_type_3(payload)
    elif location == 'jabar' and topic_parts[2] == 'aws':
        try:
            parsed_message = parse_message_type_4(json.loads(payload))
        except (json.JSONDecodeError, TypeError):
            print("Payload is not in JSON format for Type 4 message.")
            return
    elif location == 'other_type':
        try:
            parsed_message = parse_message_type_5(json.loads(payload))
        except (json.JSONDecodeError, TypeError):
            print("Payload is not in JSON format for Type 5 message.")
            return
       
    else:
        # Skip processing for unknown message type
        return

    if isinstance(parsed_message, dict):
        print(f"Parsed message from topic {topic}:")
        print(parsed_message)
    else:
        print(f"Ignored message from topic {topic}: Payload is not a dictionary.")

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
