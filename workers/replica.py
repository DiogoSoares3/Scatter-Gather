import socket
import json
from multiprocessing import Process
import os
import re


def search_files(words, database):
    results = {}
    for file in os.listdir(database): # Itera sobre todos os arquivos
        count = 0
        try:
            with open(database + "/" + file, 'r', encoding='utf-8') as f:
                raw_text = f.read()
                text = re.sub("[.,]", "", raw_text)
                for word in words: # Para cada palavra chave recebida pelo nó raiz, conta a quantidade de aparições no arquivo
                    count += text.lower().count(word.lower())
            results[file] = count
        except FileNotFoundError:
            continue
    return results


def handle_root(root_socket, database):
    while True:
        try:
            data = root_socket.recv(1024).decode('utf-8') # Recebe o subcojunto de palavras chave da raiz

            if not data: # Se não receber mais requisições, encerra a conexão
                print("No data received, breaking the loop.")
                break

            words = json.loads(data)
            results = search_files(words, database) # Faz a busca nos arquivos para ver as ocorrências dessas palavras chave

            root_socket.send(json.dumps(results).encode('utf-8')) # Responde à raiz enviando os arquivos e suas respectivas ocorrências

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            break

        except Exception as e:
            print(f"Error handling root: {e}")
            break

    root_socket.close()


def main(config_file):
    
    # Obtém configurações da réplica
    with open(config_file, 'r') as f:
        config = json.load(f)

    address = config['address']
    port = config['port']
    database = config['database_directory']
    
    # Cria um socket para o nó replica se conectar com o nó raiz
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((address, port))
    server.listen(5)
    print(f"Replica listening on {address}:{port}")

    while True:
        root_socket, addr = server.accept() # Aceita conexões das threads da raiz
        print(f"Accepted connection from {addr}")
        root_handler = Process(target=handle_root, args=(root_socket, database)) # Cria um processo para lidar com as requisições da raiz
        root_handler.start()


if __name__ == "__main__":
    import sys
    main(sys.argv[1])