#!/bin/bash
# Stop collector + children, install the new plan, migrate run IDs, restart the collector.
for pid in $(pgrep -f "bench/collect.py run"); do kill "$pid"; done
sleep 1
pkill -f "bench/spmf.jar run"; pkill -f "release/air-huim mine"
sleep 1
cp /home/pi/bench/plan_full2.json /home/pi/bench/plan.json
python3 /home/pi/bench/migrate_ids.py
cd /home/pi/bench && setsid nohup python3 /home/pi/air-huim/bench/collect.py run /home/pi/bench/plan.json --data /home/pi/bench/data >> /home/pi/bench/collect.log 2>&1 < /dev/null &
sleep 15
python3 /home/pi/air-huim/bench/collect.py status /home/pi/bench/plan.json --data /home/pi/bench/data
tail -4 /home/pi/bench/collect.log | cut -c1-170
