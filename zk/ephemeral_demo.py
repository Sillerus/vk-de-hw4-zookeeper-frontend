"""Этап 2, эфемерный узел. «Окно 2» - интерактивный zkCli, которому команды подаются
через stdin; «окно 1» - отдельные запуски zkCli для проверки.

Опыт А: в окне 2 создать эфемерный узел и выйти командой quit (сессия закрывается сразу).
Опыт Б: в окне 2 создать узел и «закрыть окно» (kill -9 процесса zkCli, как при закрытии
терминала): клиент не успевает закрыть сессию, и узел живёт до истечения тайм-аута сессии.

  python ephemeral_demo.py > ../logs/stage2_ephemeral.txt
"""
import os
import re
import signal
import subprocess
import time
from datetime import datetime

BASE = os.environ.get("ZK_BASE", "/opt/hw4-zk")
CLI = f"{BASE}/zk1/bin/zkCli.sh"
ALL = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
NODE = "/hw4-ephemeral"
NOISE = re.compile(r"^(Connecting to|WATCHER::|WatchedEvent|Welcome|JLine)|log4j|SLF4J|\[myid:|^\s*$")


def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def window1(*cmd):
    """Одна команда zkCli в «окне 1» (отдельный процесс, отдельная сессия)."""
    print(f"\n[{ts()}] окно 1 $ zkCli.sh -server {ALL} {' '.join(cmd)}", flush=True)
    out = subprocess.run([CLI, "-server", ALL, *cmd], capture_output=True, text=True, timeout=60)
    lines = [l for l in (out.stdout + out.stderr).splitlines() if not NOISE.search(l)]
    print("\n".join(lines), flush=True)
    return "\n".join(lines)


def open_window2(timeout_ms):
    print(f"\n[{ts()}] окно 2 $ zkCli.sh -timeout {timeout_ms} -server {ALL}   (интерактивно)", flush=True)
    return subprocess.Popen([CLI, "-timeout", str(timeout_ms), "-server", ALL], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                            start_new_session=True)   # своя группа процессов, как у окна терминала


def type_in(p, cmd):
    print(f"[{ts()}] окно 2 > {cmd}", flush=True)
    p.stdin.write(cmd + "\n")
    p.stdin.flush()
    time.sleep(2)


def show_window2(p):
    text = p.stdout.read()
    lines = [l for l in text.splitlines() if not NOISE.search(l) and "zk:" in l or "Created" in l or "Node" in l]
    print("    вывод окна 2:\n" + "\n".join("    | " + l for l in lines), flush=True)


def node_exists():
    out = subprocess.run([CLI, "-server", ALL, "stat", NODE], capture_output=True, text=True, timeout=60)
    return "ephemeralOwner" in out.stdout


print("=== Опыт А: создать эфемерный узел в окне 2 и выйти командой quit")
w2 = open_window2(10000)
time.sleep(4)
type_in(w2, f'create -e {NODE} "ephemeral from window 2"')
window1("stat", NODE)
window1("ls", "/")
type_in(w2, "quit")
w2.wait(timeout=20)
print(f"[{ts()}] окно 2 закрыто командой quit, код {w2.returncode}")
show_window2(w2)
window1("ls", "/")

print("\n=== Опыт Б: создать узел и закрыть окно 2 без quit (kill -9), тайм-аут сессии 10 с")
w2 = open_window2(10000)
time.sleep(4)
type_in(w2, f'create -e {NODE} "ephemeral from window 2, killed"')
window1("stat", NODE)
os.killpg(w2.pid, signal.SIGKILL)       # закрытие окна: убить zkCli.sh и его java
t_kill = time.time()
print(f"\n[{ts()}] окно 2 закрыто: SIGKILL группе процессов zkCli (pgid {w2.pid}), сессия не закрыта клиентом")
show_window2(w2)
while True:
    alive = node_exists()
    print(f"[{ts()}] окно 1: узел {NODE} {'ещё существует' if alive else 'ИСЧЕЗ'} "
          f"(прошло {time.time() - t_kill:.1f} с)", flush=True)
    if not alive:
        break
    time.sleep(1)
window1("ls", "/")
