#!/usr/bin/env bash
# Полный прогон ДЗ 4. Перед запуском: RustFS с данными ДЗ 3 (бакет hh-lake) и Java 17.
set -uo pipefail
cd "$(dirname "$0")"
source zk/env.sh
# чистый старт ансамбля
for n in 1 2 3; do [ -x "$ZK_BASE/zk$n/bin/zkServer.sh" ] && "$ZK_BASE/zk$n/bin/zkServer.sh" stop >/dev/null 2>&1; done
rm -rf logs/*
zk/setup_ensemble.sh > logs/stage1_setup.txt 2>&1
zk/stage2_ensemble.sh > logs/stage2_ensemble.txt 2>&1
python3 zk/ephemeral_demo.py > logs/stage2_ephemeral.txt 2>&1
# журналы серверов ZooKeeper (logback FILE с дозаписью, см. zk/conf/logback.xml)
mkdir -p logs/zk-server
for n in 1 2 3; do cp "$ZK_BASE"/zk$n/logs/zookeeper-*-server-*.log logs/zk-server/zk$n.log; done
coordinator/run_stage3_4.sh > logs/stage3_4_console.txt 2>&1
# дополнительные замеры времени смены лидера
for i in 1 2 3; do (cd coordinator && python3 scenario.py --run failover$i --fencing 1) >> logs/failover_repeats_console.txt 2>&1; done
