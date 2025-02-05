import socket
import struct


def create_broker():
    kafka_broker = socket.create_server(address=("localhost", 9092), family=socket.AF_INET,reuse_port=True)
    return kafka_broker

def create_response(message_body):
    request_api_key = struct.unpack(">h", message_body[:2])[0]
    print(f'header: {request_api_key}')
    request_api_version = message_body[3:5]
    print(f'request_api_version: {struct.unpack(">h",request_api_version)[0]}')
    correlation_id = message_body[6:10]
    print(f'correlation_id: {struct.unpack(">i",correlation_id)[0]}')
    correlation_id = struct.unpack(">i",correlation_id)[0]
    header = struct.pack('>i',correlation_id)
    full_message = message_body + header
    client_message = struct.pack('>i', len(full_message)) + full_message
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

    
