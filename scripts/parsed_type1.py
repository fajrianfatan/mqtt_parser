def convert_payload(payload):
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

# Test the function
original_payload = {"date": "2023-07-21", "time": "03:58:00", "id": "150013", "site": "Buay Madang", "rr": 0, "batt": 13.38, "log_temp": 42.65}
converted_payload = convert_payload(original_payload)
print(converted_payload)
