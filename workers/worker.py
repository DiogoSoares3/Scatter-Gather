import socket
import json
from multiprocessing import Process
import os
import re


def search_files(words, database):
    results = {}
    for file in os.listdir(database): # Iterates over all files
        count = 0
        try:
            with open(database + "/" + file, 'r', encoding='utf-8') as f:
                raw_text = f.read()
                text = re.sub("[.,]", "", raw_text)
                for word in words: # For each keyword received by the root node, it counts the number of appearances in the file
                    count += text.lower().count(word.lower())
            results[file] = count
        except FileNotFoundError:
            continue
    return results


def handle_root(root_socket, database):
    while True:
        try:
            data = root_socket.recv(1024).decode('utf-8') # Receives the subset of keywords from the root

            if not data: # If no more requests are received, the connection is closed
                print("No data received, breaking the loop.")
                break

            words = json.loads(data)
            results = search_files(words, database) # Search the archives to see the occurrences of these keywords

            root_socket.send(json.dumps(results).encode('utf-8')) # Responds to the root by sending the files and their respective occurrences

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            break

        except Exception as e:
            print(f"Error handling root: {e}")
            break

    root_socket.close()


def main(config_file):
    # Get worker settings
    with open(config_file, 'r') as f:
        config = json.load(f)

    address = config['address']
    port = config['port']
    database = config['database_directory']
    
    # Creates a socket for the worker node to connect to the root node
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((address, port))
    server.listen(5)
    print(f"Worker listening on {address}:{port}")

    while True:
        try:
            root_socket, addr = server.accept() # Accepts connections from root threads
            print(f"Accepted connection from {addr}")
            root_handler = Process(target=handle_root, args=(root_socket, database)) # Creates a process to handle requests from the root
            root_handler.start()
        except KeyboardInterrupt:
            root_socket.close()


if __name__ == "__main__":
    import sys
    main(sys.argv[1])