#!/usr/bin/env bash

python3 server/root_node.py &

python3 workers/replica.py workers/replica1_config.json &
python3 workers/replica.py workers/replica2_config.json &
python3 workers/replica.py workers/replica3_config.json &

sleep 1

python3 client/client.py &

wait && exit 0
