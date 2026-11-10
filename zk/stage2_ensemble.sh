#!/usr/bin/env bash
# Этап 2: запуск ансамбля, роли, потеря одного и двух серверов, повторный запуск.
# Вывод всех команд пишется в ../logs/stage2_ensemble.txt
set -uo pipefail
cd "$(dirname "$0")"
source ./env.sh
ZK() { "$ZK_BASE/zk$1/bin/zkServer.sh" "${@:2}"; }
CLI() { timeout "${CLI_TIMEOUT:-25}" "$ZK_BASE/zk1/bin/zkCli.sh" -timeout 5000 -server "$1" "${@:2}" 2>&1 \
          | grep -v -E "^(Connecting to|WATCHER::|WatchedEvent|$)|log4j|SLF4J|^Welcome|JLine|\[myid:"; }
run() { echo; echo "[$(date +%H:%M:%S)] \$ $*"; eval "$@"; }
status_all() { for n in 1 2 3; do echo "--- zk$n (порт 218$n)"; ZK $n status 2>&1 | grep -E "Mode|Error|not running|Client port"; done; }

echo "=== 1. Запуск трёх серверов"
for n in 1 2 3; do run "ZK $n start"; done
sleep 6
run status_all
run "ps -o pid=,args= -C java | grep QuorumPeerMain | sed -E 's|^ *([0-9]+) ([^ ]+java) .* ([^ ]+zoo.cfg)$|pid \\1  \\2  \\3|'"
run 'for p in 2181 2182 2183; do echo "--- $p"; echo srvr | nc -q 2 127.0.0.1 $p | grep -E "version|Mode|Zxid|Node count"; done'
run 'CLI $ZK_ALL create /hw4-ensemble "created with 3 servers"'
run 'CLI 127.0.0.1:2183 get /hw4-ensemble'

LEADER=$(for n in 1 2 3; do ZK $n status 2>&1 | grep -q "Mode: leader" && echo $n; done)
F=$(for n in 1 2 3; do [ "$n" != "$LEADER" ] && echo $n; done | head -1)
echo; echo "лидер: zk$LEADER"

echo; echo "=== 2. Остановка одного сервера (останавливаю лидера zk$LEADER)"
run "ZK $LEADER stop"
sleep 4
run status_all
run 'CLI $ZK_ALL set /hw4-ensemble "updated with 2 of 3 servers"'
run 'CLI $ZK_ALL get /hw4-ensemble'

NEWLEADER=$(for n in 1 2 3; do ZK $n status 2>&1 | grep -q "Mode: leader" && echo $n; done)
echo; echo "=== 3. Остановка второго сервера (zk$NEWLEADER), работает только один из трёх"
run "ZK $NEWLEADER stop"
sleep 4
LAST=$(for n in 1 2 3; do [ "$n" != "$LEADER" ] && [ "$n" != "$NEWLEADER" ] && echo $n; done)
run status_all
run "echo srvr | nc -q 2 127.0.0.1 218$LAST"
run "ps -o args= -C java | grep QuorumPeerMain | grep -o -E 'zk[0-9]/(bin/\\.\\./)?conf/zoo.cfg' | sort -u"
run "CLI_TIMEOUT=20 CLI 127.0.0.1:218$LAST get /hw4-ensemble; echo \"код выхода: \$?\""

echo; echo "=== 4. Повторный запуск остановленных серверов"
run "ZK $LEADER start"
run "ZK $NEWLEADER start"
sleep 8
run status_all
run 'CLI $ZK_ALL get /hw4-ensemble'
run 'CLI $ZK_ALL delete /hw4-ensemble'
