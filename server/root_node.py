import socket
import json
from threading import Thread, Lock
from multiprocessing import Process
from collections import defaultdict


def gather(worker_socket, lock, results, i):
    worker_result = json.loads(worker_socket.recv(4096).decode('utf-8')) #  Receives related files and their occurrences
    print(f"Data received from worker {i}: {worker_result}")

    with lock:  # Mutual exclusion when modifying the results variable
        for file, count in worker_result.items():
            results[file] += count  # Adds the number of occurrences to the total for each of the files
                

def scatter(worker_socket, words_subset, i):
    print(f"Sending data to worker {i}: {words_subset}")
    worker_socket.send(json.dumps(words_subset).encode('utf-8')) # Send the subset of keywords


def handle_client(client_socket):
    while True:
        try:
            query = client_socket.recv(1024).decode('utf-8') # Receives the request from the client

            if not query: # If the client socket doesn't send anything else, it breaks the loop and the thread is automatically deleted
                break
            
            print(f"Received query from client: {query}")
            results = defaultdict(int)
            lock = Lock()

            words = query.split() # Separate into keywords
            num_workers = len(WORKERS)

            threads_scatter_and_gather = []
            for i, worker in enumerate(WORKERS):
                words_subset = words[i::num_workers] # Each worker is assigned a subset of keywords from the client's request
                
                worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Create a new socket to connect to a worker
                worker_socket.connect((worker['address'], worker['port']))
                
                # Spreading the messages to the workers (Scatter)
                ts = Thread(target=scatter, args=(worker_socket, words_subset, i,))
                ts.start()
                threads_scatter_and_gather.append(ts)

                # Gathering the information returned by each worker (Gather)
                tg = Thread(target=gather, args=(worker_socket, lock, results, i,))
                tg.start()
                threads_scatter_and_gather.append(tg)

            for t in threads_scatter_and_gather:
                t.join()

            client_socket.send(json.dumps(results).encode('utf-8')) # Send the answer to the client
        
        except Exception as e:
            print(f"Error handling client: {e}")
            break

    client_socket.close()


def main():
    # Atribuindo um socket ao nó raiz que ficará escutando clientes e workers
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ADRESS, PORT))
    server.listen(5)
    print(f"Root node listening on {ADRESS}:{PORT}")
    
    # Para cada conexão de um cliente é atribuido um novo socket. A troca de mensagem entre o socket do servidor e do cliente serão feitas por uma thread
    while True:
        try:
            client_socket, addr = server.accept() # Faz a conexão com o cliente
            print(f"Accepted connection from {addr}")
            client_handler = Process(target=handle_client, args=(client_socket,)) # Atribui uma thread para lidar com o cliente
            client_handler.start()
        except KeyboardInterrupt:
            client_socket.close()


if __name__ == "__main__":
    with open('server/root_node_config.json', 'r') as f:
        config = json.load(f)
        
    ADRESS = config['address']
    PORT = config['port']
    WORKERS = config['workers']

    main()