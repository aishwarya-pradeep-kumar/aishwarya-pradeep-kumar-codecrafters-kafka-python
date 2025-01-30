from app.broker import create_broker, send_response


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    kafka_broker = create_broker()
    while True:
        client, address = kafka_broker.accept()
        send_response(client)
    

if __name__ == "__main__":
    main()
