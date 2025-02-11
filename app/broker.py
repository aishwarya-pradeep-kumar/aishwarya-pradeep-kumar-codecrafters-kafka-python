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
    if api_versions.check_api_version(message_body):
        client_message = correlation_id + struct.pack('>h', 35)
    else:
        client_message = correlation_id
    return struct.pack('>i', len(client_message))+client_message


def send_response(client):
    try:
        client_message = client.recv(1024)
        response = create_response(client_message)
        client.send(response)
    except Exception as ex:
        print(f'Error occured: {ex}')
    finally:
        client.close()

    
