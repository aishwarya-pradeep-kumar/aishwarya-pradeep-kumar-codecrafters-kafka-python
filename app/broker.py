import socket
import struct


def create_broker():
    kafka_broker = socket.create_server(address=("localhost", 9092), family=socket.AF_INET,reuse_port=True)
    return kafka_broker

def create_response(message_body):
    header_size = struct.calcsize("I")
    header = struct.unpack("I", message_body[:header_size])[0]
    print(f'header: {header}')
    rest_body = message_body[header_size:]
    print(f'rest_body: {rest_body}')
    header = struct.pack('>i',7)
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

    
