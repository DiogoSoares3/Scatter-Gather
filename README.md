# Text Retrieval Service with the Scatter/Gather Pattern for Dividing the Work

A text retrieval system is a type of information retrieval system specifically designed to find and retrieve relevant documents or text snippets from a larger collection based on user queries.

For this task, you must implement a text retrieval service using the scatter/gather pattern. A client makes query requests with a search string to the root node of the system. The root node divides the search string into a set of keywords and sends equal portions of the total keywords to the replicas, which store the plain text files. Each replica responds to the root node with the files and the number of occurrences of keywords. The root node combines the responses from the replicas and replies to the clients with a list of texts, along with the number of occurrences.

Other requirements:

- Communication must be implemented using sockets or MPI. If you choose to use MPI, collective communication resources or MPI implementations of patterns such as scatter/gather and map/reduce cannot be used.

- Each communication performed must be displayed on the screen (print).

- For testing and simulation, configuration files should be created:

    - For the replicas: only their address is needed.

    - The root node needs its own address and the addresses of the replicas.

    - The clients need the address of the root node and a list of requests that will be made during the simulation.

    - There must be text files available for the system to search.

    - Requests should be made at random intervals of 1 to 2 seconds.

### How to run the code

You need to initialize at 5 instances of terminals. One to run the root node, other 3 to run the workers, and the other one to run the client.

Fom the root of the project:

- Run the root node:
    ```
    python3 server/root_node.py
    ```
- Run the three workers in 3 differente terminal instances:
    ```
    python3 workers/worker.py workers/worker1_config.json 
    ```

    ```
    python3 workers/worker.py workers/worker2_config.json 
    ```

    ```
    python3 workers/worker.py workers/worker3_config.json 
    ```

    Note that json file in the second argument is the configuration file of worker.

- Run the client:
    ```
    python3 client/client.py
    ```
