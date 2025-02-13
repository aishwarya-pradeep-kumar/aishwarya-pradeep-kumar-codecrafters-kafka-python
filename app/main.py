from app.broker import create_broker, send_response
import threading


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    while True:
        kafka_broker = create_broker()
        client, address = kafka_broker.accept()
        while True:
            client_thread = threading.Thread(target=send_response, args=(client))
            client_thread.start()

    

if __name__ == "__main__":
    main()
