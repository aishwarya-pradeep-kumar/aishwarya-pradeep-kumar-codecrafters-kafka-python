import socket
import struct
import app.api_versions as api_versions


def create_broker():
    kafka_broker = socket.create_server(address=("localhost", 9092), family=socket.AF_INET,reuse_port=True)
    return kafka_broker

def create_response(message_body):
    # message_size = struct.unpack(">i", message_body[:4])[0]
    # print(f'message_size: {message_size}')
    # request_api_key = struct.unpack(">h", message_body[4:6])[0]
    # print(f'request_api_key: {request_api_key}')
    # request_api_version = message_body[6:8]
    # print(f'request_api_version: {struct.unpack(">h",request_api_version)[0]}')
    correlation_id = message_body[8:12]
    error_code = api_versions.check_api_version(message_body)
    # full_message = message_body + correlation_id
    client_message = struct.pack('>i', len(correlation_id)) + correlation_id + error_code
    return client_message


def send_response(client):
    try:
        client_message = client.recv(1024)
        response = create_response(client_message)
        client.send(response)
    except Exception as ex:
        print(f'Error occured: {ex}')
    finally:
        client.close()

    
