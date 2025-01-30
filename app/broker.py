import socket
import struct

CORRELATION_ID=7

def create_broker():
    kafka_broker = socket.create_server(address=("localhost", 9092), family=socket.AF_INET,reuse_port=True)
    return kafka_broker

def create_response(message_body, correlation_id):
    message_body = message_body.encode('utf-8')
    header = struct.pack('>i',correlation_id)
    full_message = message_body + header
    client_message = struct.pack('>i', len(full_message)) + full_message
    return client_message


def send_response(client):
    try:
        client_message = client.recv(1024).decode('utf-8')
        print(f'client message: {client_message}')
        response = create_response("", 7)
        client.send(response)
    except Exception as ex:
        print(f'Error occured: {ex}')
    finally:
        client.close()

    
