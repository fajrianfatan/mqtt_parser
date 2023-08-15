import json

def parse_message_type_2(payload):
    try:
        message = json.loads(payload)

        # Parse 'time' to RFC 3339 format
        time_str = message['data'][0]['time']
        formatted_time = f"{time_str}Z"

        # Extract other fields
        rr = message['data'][0]['vals'][15]
        bt = message['data'][0]['vals'][1]  # Change 'batt' to 'bt'
        log_temp = message['data'][0]['vals'][3]

        # Extract additional fields from the 'head' section
        id = message['head']['environment']['serial_no']
        site = message['head']['environment']['station_name']

        # Create the parsed payload
        parsed_payload = {
            'time': formatted_time,
            'rr': rr,
            'bt': bt,  # Use 'bt' instead of 'batt'
            'log_temp': log_temp,
            'id': id,
            'site': site
        }

        return parsed_payload

    except json.JSONDecodeError as e:
        print(f"Error decoding Type 2 JSON: {e}")
        return None

# Test the function with the given JSON payload
payload = '{"head": {"transaction": 0, "signature": 27629, "environment": {"station_name": "Digital Climate Station", "table_name": "mqtt1", "model": "CR1000X", "serial_no": "25317", "os_version": "CR1000X.Std.06.01", "prog_name": "CPU:Digital_Climate_v5.5 (Pipeline Mode)_str.CR1X"}, "fields": [{"name": "times", "type": "xsd:string", "process": "Smp", "settable": false, "string_len": 24}, {"name": "batt_volt", "type": "xsd:float", "units": "V", "process": "Smp", "settable": false}, {"name": "Lithium_Battery", "type": "xsd:float", "units": "V", "process": "Smp", "settable": false}, {"name": "PTemp", "type": "xsd:float", "units": "degC", "process": "Smp", "settable": false}, {"name": "TA_1m", "type": "xsd:float", "units": "degC", "process": "Smp", "settable": false}, {"name": "TA_4m", "type": "xsd:float", "units": "degC", "process": "Smp", "settable": false}, {"name": "TA_7m", "type": "xsd:float", "units": "degC", "process": "Smp", "settable": false}, {"name": "TA_10m", "type": "xsd:float", "units": "degC", "process": "Smp", "settable": false}, {"name": "RH_1m", "type": "xsd:float", "units": "%", "process": "Smp", "settable": false}, {"name": "RH_4m", "type": "xsd:float", "units": "%", "process": "Smp", "settable": false}, {"name": "RH_7m", "type": "xsd:float", "units": "%", "process": "Smp", "settable": false}, {"name": "RH_10m", "type": "xsd:float", "units": "%", "process": "Smp", "settable": false}, {"name": "PA_meas", "type": "xsd:float", "units": "mbar", "process": "Smp", "settable": false}, {"name": "SR_meas", "type": "xsd:float", "units": "W/m^2", "process": "Smp", "settable": false}, {"name": "SR_Max", "type": "xsd:float", "units": "W/m^2", "process": "Smp", "settable": false}, {"name": "PR_meas_Total", "type": "xsd:float", "units": "mm", "process": "Smp", "settable": false}]}, "data": [{"time": "2023-07-21T03:59:00", "vals": ["2023-07-21 03:59:00", 13.32, 3.732, 39.76, 28.92, 30.77, 29.87, 29.73, 57.46, 59.91, 57.72, 56.45, 991.4682, 580.0554, 1050.364, 0]}]}'
parsed_payload = parse_message_type_2(payload)
print(parsed_payload)
