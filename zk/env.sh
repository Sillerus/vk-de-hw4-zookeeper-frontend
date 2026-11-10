# Общие переменные для скриптов этапов 1-2
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64   # задание требует Java 17
export PATH="$JAVA_HOME/bin:$PATH"
export ZK_BASE=${ZK_BASE:-/opt/hw4-zk}
export ZK_TARBALL=${ZK_TARBALL:-apache-zookeeper-3.9.6-bin.tar.gz}
export ZK_ALL=127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183
export USER=${USER:-$(whoami)}
unset JAVA_TOOL_OPTIONS   # прокси-настройки JVM этой машины засоряют вывод zkCli
