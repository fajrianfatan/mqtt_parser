def convert_payload(payload):
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

# Test the function
original_payload = {"date": "2023-07-21", "time": "04:00:00", "id": "STA9001", "site": "ARG REKAYASA KEMAYORAN", "rr": "0.0", "batt": "10.85", "shunt": "-0.04", "curr": "-421.40", "log_temp": "49.66"}
converted_payload = convert_payload(original_payload)
print(converted_payload)
