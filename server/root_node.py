import socket
import json
from threading import Thread, Lock
from collections import defaultdict


def query_replica(replica, words_subset, lock, results):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: # Cria um novo socket para se conectar a uma replica
        s.connect((replica['address'], replica['port']))
        
        print(f"Data send to replica: {words_subset}")
        s.send(json.dumps(words_subset).encode('utf-8')) # Envia o subconjunto de palavras chave

        replica_result = json.loads(s.recv(4096).decode('utf-8')) # Recebe os arquivos relacionados e as suas ocorrências
        print(f"Data received from replica: {replica_result}")

        with lock:  # Exclusão mútua ao modificar a variável results
            for file, count in replica_result.items():
                results[file] += count  # Adiciona o número de ocorrências ao total de cada um dos arquivos


def handle_client(client_socket, replicas):
    while True:
        try:
            query = client_socket.recv(1024).decode('utf-8') # Recebe a requisição do cliente

            if not query: # Se o socket do cliente não enviar mais nada, quebra o loop e a thread é automaticamente eliminada
                break

            print(f"Received query from client: {query}")
            words = query.split() # Separa em palavras chave
            num_replicas = len(replicas)
            
            results = defaultdict(int)
            lock = Lock()

            threads = []
            for i, replica in enumerate(replicas):
                words_subset = words[i::num_replicas]  # Para cada replica é atribuida um subconjunto de palavras chave da requisição do cliente

                t = Thread(target=query_replica, args=(replica, words_subset, lock, results)) # Uma thread para cada replica
                t.start()
                threads.append(t)

            for t in threads:
                t.join() # Espera todas as replicas enviarem suas repostas
            
            client_socket.send(json.dumps(results).encode('utf-8')) # Envia a resposta para o cliente
        
        except Exception as e:
            print(f"Error handling client: {e}")
            break


def main():
    with open('server/root_node_config.json', 'r') as f:
        config = json.load(f)
    
    # Pegando as configurações do nó raiz
    address = config['address']
    port = config['port']
    replicas = config['replicas']
    
    # Atribuindo um socket ao nó raiz que ficará escutando clientes e replicas
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((address, port))
    server.listen(5)
    print(f"Root node listening on {address}:{port}")
    
    # Para cada conexão de um cliente é atribuido um novo socket. A troca de mensagem entre o socket do servidor e do cliente serão feitas por uma thread
    while True:
        client_socket, addr = server.accept() # Faz a conexão com o cliente
        print(f"Accepted connection from {addr}")
        client_handler = Thread(target=handle_client, args=(client_socket, replicas)) # Atribui uma thread para lidar com o cliente
        client_handler.start()
        #client_handler.join()


if __name__ == "__main__":
    main()