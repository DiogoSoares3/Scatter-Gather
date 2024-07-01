#!/usr/bin/env bash

python3 server/root_node.py &

sleep 1

python3 workers/worker.py workers/worker1_config.json &
python3 workers/worker.py workers/worker2_config.json &
python3 workers/worker.py workers/worker3_config.json &

sleep 1

python3 client/client.py &

wait && exit 0
