import json
from datetime import datetime
from parser_db import get_topic_list
# from parser_msg import parse_tipeCampbell_A,parse_tipeCampbell_B,parse_tipeA,parse_tipeB,parse_tipeC,parse_tipeD,parse_tipeE,parse_tipeF # Import all your parser functions

# Function to create the parser_connector dictionary
def create_parser_connector():
    # Get the list of topics from the database
    topic_list = get_topic_list()

    # Create the parser_connector dictionary
    parser_connector = {}
    for topic_info in topic_list:
        topic_type = topic_info['type']
        if topic_info.get('parsed'):
            if topic_type == 'parse_tipeCampbell_A':
                parser_connector[topic_info['topic']] = parse_tipeCampbell_A
            elif topic_type == 'parse_tipeCampbell_B':
                parser_connector[topic_info['topic']] = parse_tipeCampbell_B
            elif topic_type == 'tipeA':
                parser_connector[topic_info['topic']] = parse_tipeA  
            elif topic_type == 'tipeB':
                parser_connector[topic_info['topic']] = parse_tipeB
            elif topic_type == 'tipeC':
                parser_connector[topic_info['topic']] = parse_tipeC
            elif topic_type == 'tipeD':
                parser_connector[topic_info['topic']] = parse_tipeD
            elif topic_type == 'tipeE':
                parser_connector[topic_info['topic']] = parse_tipeE
            elif topic_type == 'tipeF':
                parser_connector[topic_info['topic']] = parse_tipeF
            # Add more entries as needed for other topic types
            else:
                print(f"Warning: No parser function found for topic type - {topic_type}")
        else:
            print(f"Warning: Topic not marked as parsed - {topic_info['topic']}")

    return parser_connector


def parse_tipeCampbell_A(data):
    parsed = json.loads(data)

    time_str = parsed['data'][0]['time']  # Fix: Access 'time' under 'data'
    formatted_time = f"{time_str}Z"
    
    # Extract other fields
    rr = parsed['data'][0]['vals'][15]
    bt = parsed['data'][0]['vals'][1]  # Change 'batt' to 'bt'
    log_temp = parsed['data'][0]['vals'][3]

    # Extract additional fields from the 'head' section
    id = parsed['head']['environment']['serial_no']
    site = parsed['head']['environment']['station_name']

    # Create the parsed payload
    return {
        'datetime': formatted_time,
        'namaLokasi': site,
        'rr': rr,
        'batt': bt,  # Use 'bt' instead of 'batt'
        'loggerTemp': log_temp,
        'id': id,
    }

def parse_tipeCampbell_B(data):
    parsed = json.loads(data)

    results = []

    for entry in parsed['data']:
        date_str = entry['vals'][1]  # Change to correct index for 'date'
        time_str = entry['vals'][2]  # Change to correct index for 'time'

        combined_datetime = datetime.strptime(date_str + ' ' + time_str, '%d/%m/%Y %H:%M:%S')

        rfc3339_timestamp = combined_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')

        results.append({
            'datetime': rfc3339_timestamp,
            'staid': parsed['head']['environment']['serial_no'],  # Change to correct location for 'id'
            'namaLokasi': parsed['head']['environment']['station_name'],
            'rr': entry['vals'][9],
            'batt': entry['vals'][0],
            'loggerTemp': entry['vals'][15]
        })

    return results

def parse_tipeA(data):
    # {"time": "2023-07-21T03:59:00Z", "rr": 0, "batt": 13.15, "log_temp": 41.66}
    parsed = json.loads(data)
    return {
        'datetime': parsed.get('time'),
        'rr': parsed.get('rr'),
        'batt': parsed.get('batt'),
        'loggerTemp': parsed.get('log_temp')
    }

def parse_tipeB(data):
    # '{"date":"2023-08-04","time":"06:29:00","id":"STA2139","site":"AWS Prambanan","ws":1.5,"ws_max":5.5,"wd":208.3,"temp":31.27,"temp_max":31.73,"temp_min":24.23,"rh":52.53,"press":995.4,"rr":0,"sr":822.8,"sr_max":1057,"batt":12.7,"log_temp":39.98,"lithium":3.7}'
    parsed = json.loads(data)
    date_str = parsed['date']
    time_str = parsed['time']
    
    # Combine date and time into a datetime object
    combined_datetime = datetime.strptime(date_str + ' ' + time_str, '%Y-%m-%d %H:%M:%S')

    # Convert datetime to RFC 3339 format
    rfc3339_timestamp = combined_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    return {
        'datetime' : rfc3339_timestamp,
        'staid' : parsed['id'],
        'namaLokasi' : parsed['site'],
        'rr' : parsed['rr'],
        'batt' : parsed['batt'],
        'wspd' : parsed['ws'],
        'wdir' : parsed['wd'],
        'temp' : parsed['temp'],
        'rh' : parsed['rh'],
        'press' : parsed['press'],
        'sr' : parsed['sr'],
        'loggerTemp' : parsed['log_temp'],
        'lithium' : parsed['lithium']
    }

def parse_tipeC(data):
    # {"date":"2023-08-04","time":"06:40:00","id":"150020","site":"ARG Kalianda","rr":0.0,"batt":13.26,"log_temp":43.78}
    parsed = json.loads(data)
    
    date_str = parsed['date']
    time_str = parsed['time']
    
    # Combine date and time into a datetime object
    combined_datetime = datetime.strptime(date_str + ' ' + time_str, '%Y-%m-%d %H:%M:%S')

    # Convert datetime to RFC 3339 format
    rfc3339_timestamp = combined_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    return {
        'datetime' : rfc3339_timestamp,
        'staid' : parsed['id'],
        'namaLokasi' : parsed['site'],
        'rr' : parsed['rr'],
        'batt' : parsed['batt'],
        'loggerTemp' : parsed['log_temp']
    }

def parse_tipeD(data):
    # {"date": "2023-07-21", "batt": "13.5", "rr": "000.0", "time": "03:40:00"}
    # Remove backslashes and newline characters from the payload
    data = data.replace("\\", "").strip()

    # Load the JSON payload
    parsed = json.loads(data)

    # Extract date and time strings from the payload
    date_str = parsed['date']
    time_str = parsed['time']

    # Convert date and time strings to desired format ('21-07-2023T04:08:00Z')
    date_str = date_str.replace('/', '-')
    rfc3339_timestamp = f"{date_str}T{time_str}Z"

    # Create the parsed payload
    parsed_payload = {
        'datetime': rfc3339_timestamp,
        'rr': parsed.get('rr'),
        'batt': parsed.get('batt'),
    }

    return parsed_payload

def parse_tipeE(data):
    # {"date": "2023-07-21", "time": "04:00:00", "id": "STG1008", "site": "ARG REKAYASA SEPAKU SEMOI", "rr": "0.4", "batt": "12.96", "curr": "368.10", "log_temp": "60.86"}
    parsed = json.loads(data)
    
    date_str = parsed['date']
    time_str = parsed['time']
    
    # Combine date and time into a datetime object
    combined_datetime = datetime.strptime(date_str + ' ' + time_str, '%Y-%m-%d %H:%M:%S')

    # Convert datetime to RFC 3339 format
    rfc3339_timestamp = combined_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    return {
        'datetime' : rfc3339_timestamp,
        'staid' : parsed['id'],
        'namaLokasi' : parsed['site'],
        'rr' : parsed['rr'],
        'batt' : parsed['batt'],
        'curr' : parsed['curr'],
        'loggerTemp' : parsed['log_temp']
    }

def parse_tipeF(data):
    # {"date": "2023-07-21", "time": "04:00:00", "id": "STA9001", "site": "ARG REKAYASA KEMAYORAN", "rr": "0.0", "batt": "10.85", "shunt": "-0.04", "curr": "-421.40", "log_temp": "49.66"}
    parsed = json.loads(data)
    
    date_str = parsed['date']
    time_str = parsed['time']
    
    # Combine date and time into a datetime object
    combined_datetime = datetime.strptime(date_str + ' ' + time_str, '%Y-%m-%d %H:%M:%S')

    # Convert datetime to RFC 3339 format
    rfc3339_timestamp = combined_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    return {
        'datetime' : rfc3339_timestamp,
        'staid' : parsed['id'],
        'namaLokasi' : parsed['site'],
        'rr' : parsed['rr'],
        'batt' : parsed['batt'],
        'shunt' : parsed['shunt'],
        'curr' : parsed['curr'],
        'loggerTemp' : parsed['log_temp']
    }


# # Create the parser_connector dictionary
# parser_connector = create_parser_connector()
# parser_connector = {
#     "tipeCampbell_A" : parse_tipeCampbell_A,
#     "tipeCampbell_B" : parse_tipeCampbell_B,
#     "tipeA" : parse_tipeA,
#     "tipeB" : parse_tipeB,
#     "tipeC" : parse_tipeC,
#     "tipeD" : parse_tipeD,
#     "tipeE" : parse_tipeE,
#     "tipeF" : parse_tipeF,
# }