import json

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

# Process the messages
for topic, payload in messages:
    device_id = topic.split('/')[-2]
    if device_id not in device_messages:
        device_messages[device_id] = []

    device_messages[device_id].append((topic, payload))

    # Check if this is the last message for the device
    if topic.endswith('/slr_max'):
        process_device_messages(device_id)
