import socket
import struct
import app.api_versions as api_versions


def create_broker():
    kafka_broker = socket.create_server(address=("localhost", 9092), family=socket.AF_INET,reuse_port=True)
    return kafka_broker

def create_api_versions_response_message(correlation_id, error_code, message_body):
    response_header = correlation_id 
    api_key = message_body[4:6]
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    response_body = struct.pack('>h', error_code) + struct.pack('>b',0) + api_key + struct.pack('>h', min_version) + struct.pack('>h', max_version) + struct.pack('>b', 0) + struct.pack('>i', throttle_time_ms) + struct.pack('>b', 0)
    return response_header, response_body

def create_api_versions_response(message_body):
    # message_size = struct.unpack(">i", message_body[:4])[0]
    # print(f'message_size: {message_size}')
    # request_api_key = struct.unpack(">h", message_body[4:6])[0]
    # print(f'request_api_key: {request_api_key}')
    # request_api_version = message_body[6:8]
    # print(f'request_api_version: {struct.unpack(">h",request_api_version)[0]}')
    correlation_id = message_body[8:12]
    if api_versions.check_api_version(message_body):
        error_code = 0
        response_header, response_body = create_api_versions_response_message(correlation_id, error_code, message_body)
    else:
        error_code = 35
        response_header, response_body = create_api_versions_response_message(correlation_id, error_code, message_body)
    message_length = len(response_header) + len(response_body)    
    return struct.pack('>i', message_length)+response_header+response_body


def send_response(client):
    try:
        client_message = client.recv(1024)
        response = create_api_versions_response(client_message)
        client.send(response)
    except Exception as ex:
        print(f'Error occured: {ex}')
    finally:
        client.close()

    
