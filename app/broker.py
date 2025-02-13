import socket
import struct
import app.api_versions as api_version


def create_broker():
    kafka_broker = socket.create_server(address=("localhost", 9092), family=socket.AF_INET,reuse_port=True)
    return kafka_broker

def create_api_versions_response_message(api_key, correlation_id, error_code):
    response_header = correlation_id
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    print(f'api_key: {struct.unpack(">h", api_key)}')
    response_body = (
        error_code.value.to_bytes(2) 
        + int(2).to_bytes(1)
        + api_key
        + min_version.to_bytes(2)
        + max_version.to_bytes(2)
        + tag_buffer
        + throttle_time_ms.to_bytes(4)
        + tag_buffer
    )
    return response_header, response_body

def parse_client_request(request_body):
    api_key = request_body[4:6]
    api_versions = request_body[6:8]
    correlation_id = request_body[8:12]
    message_body = request_body[12:]
    return api_key, api_versions, correlation_id, message_body

def create_api_versions_response(request_body):
    api_key, api_versions, correlation_id, message_body = parse_client_request(request_body)
    print(f'api_key: {struct.unpack(">h", api_versions)}')
    if api_version.check_api_version(struct.unpack(">h", api_versions)[0]):
        error_code = 0
        response_header, response_body = create_api_versions_response_message(api_key, correlation_id, error_code)
    else:
        error_code = 35
        response_header, response_body = create_api_versions_response_message(api_key, correlation_id, error_code)
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

    
