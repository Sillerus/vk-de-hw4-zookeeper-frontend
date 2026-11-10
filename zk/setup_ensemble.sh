#!/usr/bin/env bash
# Этап 1: распаковать apache-zookeeper-3.9.6-bin.tar.gz в три копии,
# положить каждой свой conf/zoo.cfg, общий conf/logback.xml и data/myid.
set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh
run() { echo "\$ $*"; eval "$@"; }
run 'java -version 2>&1 | head -1'
run 'echo JAVA_HOME=$JAVA_HOME'
mkdir -p "$ZK_BASE"
[ -f "$ZK_TARBALL" ] || { echo "нет $ZK_TARBALL: скачайте с https://zookeeper.apache.org/releases.html"; exit 1; }
run "sha512sum $ZK_TARBALL"
for n in 1 2 3; do
  run "rm -rf $ZK_BASE/zk$n && mkdir -p $ZK_BASE/zk$n && tar xzf $ZK_TARBALL -C $ZK_BASE/zk$n --strip-components=1"
done
for n in 1 2 3; do
  run "cp conf/zoo$n.cfg $ZK_BASE/zk$n/conf/zoo.cfg"
  run "cp conf/logback.xml $ZK_BASE/zk$n/conf/logback.xml"
  run "mkdir -p $ZK_BASE/zk$n/data && echo $n > $ZK_BASE/zk$n/data/myid"
done
run 'ls $ZK_BASE $ZK_BASE/zk1'
run 'for n in 1 2 3; do echo "zk$n/data/myid = $(cat $ZK_BASE/zk$n/data/myid)"; done'
run 'diff <(grep ^server $ZK_BASE/zk1/conf/zoo.cfg) <(grep ^server $ZK_BASE/zk2/conf/zoo.cfg) && diff <(grep ^server $ZK_BASE/zk1/conf/zoo.cfg) <(grep ^server $ZK_BASE/zk3/conf/zoo.cfg) && echo "строки server.N во всех трёх zoo.cfg одинаковые"'
run 'grep -H -E "^(dataDir|clientPort|admin.serverPort)" $ZK_BASE/zk*/conf/zoo.cfg'
