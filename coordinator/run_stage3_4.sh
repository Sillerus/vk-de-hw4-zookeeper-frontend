#!/usr/bin/env bash
# Этапы 3 и 4: два запуска сценария (без защиты и с защитой по эпохе)
# и состояние указателя глазами zkCli. Нужны: ансамбль ZooKeeper и RustFS с данными ДЗ 3.
set -uo pipefail
cd "$(dirname "$0")"
source ../zk/env.sh
CLI() { "$ZK_BASE/zk1/bin/zkCli.sh" -server "$ZK_ALL" "$@" 2>&1 | grep -v -E "^(Connecting to|WATCHER::|WatchedEvent|$)|log4j|SLF4J|\[myid:"; }
for run in nofence fencing; do
  f=0; [ "$run" = fencing ] && f=1
  echo "\$ FENCING=$f python scenario.py --run $run --fencing $f"
  python3 scenario.py --run $run --fencing $f
  { echo "\$ zkCli.sh -server \$ZK_ALL get -s /hw4/$run/result"; CLI get -s /hw4/$run/result
    echo; echo "\$ zkCli.sh -server \$ZK_ALL get -s /hw4/$run/epoch"; CLI get -s /hw4/$run/epoch
    echo; echo "\$ zkCli.sh -server \$ZK_ALL ls /hw4/$run/checkpoint"; CLI ls /hw4/$run/checkpoint; } \
    | tee ../logs/$run/zkcli_pointer.txt
done
