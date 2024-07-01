import socket
import json
import time
import random


def main():
    with open('client/client_config.json', 'r') as f:
        config = json.load(f) 
    
    # Get the settings from the .json configuration file
    root_address = config['root_address']
    root_port = config['root_port']
    requests = config['requests']
    
    # When the client makes a new request, a new socket is created, simulating diferent client connections
    for query in requests:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((root_address, root_port))
            print(f"Sending query to root: {query}")
            client.send(query.encode('utf-8'))
            response = json.loads(client.recv(4096).decode('utf-8'))
            print(f"Received response: {response}")

        time.sleep(random.randint(1, 2))


if __name__ == "__main__":
    main()