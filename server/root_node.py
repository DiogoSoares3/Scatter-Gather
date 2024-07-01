import socket
import json
from threading import Thread, Lock
from multiprocessing import Process
from collections import defaultdict


def gather(replica_socket, lock, results, i):
    replica_result = json.loads(replica_socket.recv(4096).decode('utf-8')) # Recebe os arquivos relacionados e as suas ocorrências
    print(f"Data received from replica {i}: {replica_result}")

    with lock:  # Exclusão mútua ao modificar a variável results
        for file, count in replica_result.items():
            results[file] += count  # Adiciona o número de ocorrências ao total de cada um dos arquivos
                

def scatter(replica_socket, words_subset, i):
    print(f"Sending data to replica {i}: {words_subset}")
    replica_socket.send(json.dumps(words_subset).encode('utf-8')) # Envia o subconjunto de palavras chave


def handle_client(client_socket):
    while True:
        try:
            query = client_socket.recv(1024).decode('utf-8') # Recebe a requisição do cliente

            if not query: # Se o socket do cliente não enviar mais nada, quebra o loop e a thread é automaticamente eliminada
                break
            
            print(f"Received query from client: {query}")
            results = defaultdict(int)
            lock = Lock()

            words = query.split() # Separa em palavras chave
            num_replicas = len(REPLICAS)

            threads_scatter_and_gather = []
            for i, replica in enumerate(REPLICAS):
                words_subset = words[i::num_replicas] # Para cada replica é atribuida um subconjunto de palavras chave da requisição do cliente
                
                replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Cria um novo socket para se conectar a uma replica
                replica_socket.connect((replica['address'], replica['port']))
                
                # Dispersando o envio de mensagens para as réplicas (Scatter)
                ts = Thread(target=scatter, args=(replica_socket, words_subset, i,))
                ts.start()
                threads_scatter_and_gather.append(ts)

                # Reunindo as informações retornadas por cada réplica (Gather)
                tg = Thread(target=gather, args=(replica_socket, lock, results, i,))
                tg.start()
                threads_scatter_and_gather.append(tg)

            for t in threads_scatter_and_gather:
                t.join()

            client_socket.send(json.dumps(results).encode('utf-8')) # Envia a resposta para o cliente
        
        except Exception as e:
            print(f"Error handling client: {e}")
            break

    client_socket.close()


def main():
    # Atribuindo um socket ao nó raiz que ficará escutando clientes e replicas
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
    REPLICAS = config['replicas']

    main()