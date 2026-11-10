"""Сценарий этапов 3 и 4: две копии координатора, приостановка лидера посреди расчёта.

  python scenario.py --run nofence --fencing 0     # этап 3
  python scenario.py --run fencing --fencing 1     # этап 4

Шаги:
  1. очистить /hw4/<run> в ZooKeeper и s3://hh-lake/hw4/<run>/, создать указатель
     /hw4/<run>/result (версия 0) и узел эпохи /hw4/<run>/epoch;
  2. запустить coord-A, через 1 с coord-B (тайм-аут сессии 10 с у обоих);
  3. когда в чекпоинте наберётся PAUSE_AT чанков, отправить лидеру SIGSTOP;
  4. держать лидера на паузе PAUSE_S секунд (запасной за это время доделывает расчёт),
     и не меньше, чем нужно запасному на завершение расчёта, затем SIGCONT;
  5. дождаться завершения обоих и вывести итог: строки «записал результат»,
     BadVersionError, версию и содержимое указателя, время смены лидера.
Журнал сценария: logs/<run>/scenario.log, журналы координаторов: logs/<run>/coord-*.log.
"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(HERE))
import coordinator as C                       # noqa: E402  (подхватывает .env и код ДЗ 3)
from kazoo.client import KazooClient          # noqa: E402

PAUSE_AT = int(os.environ.get("PAUSE_AT", "24"))
PAUSE_S = float(os.environ.get("PAUSE_S", "40"))
TS = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3})")


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def parse_ts(line):
    return datetime.strptime(TS.match(line).group(1), "%Y-%m-%d %H:%M:%S.%f").timestamp()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", required=True)
    ap.add_argument("--fencing", type=int, choices=[0, 1], required=True)
    a = ap.parse_args()
    logdir = REPO / "logs" / a.run
    logdir.mkdir(parents=True, exist_ok=True)
    for f in logdir.glob("*.log"):
        f.unlink()
    out = open(logdir / "scenario.log", "w", encoding="utf-8")

    def say(msg):
        line = f"{now()} [scenario] {msg}"
        print(line, flush=True)
        out.write(line + "\n")
        out.flush()

    base = f"/hw4/{a.run}"
    zk = KazooClient(hosts=C.ZK_HOSTS, timeout=10)
    zk.start(timeout=15)
    if zk.exists(base):
        zk.delete(base, recursive=True)
    zk.ensure_path(f"{base}/checkpoint")
    zk.create(f"{base}/epoch", b"")
    zk.create(f"{base}/result", b"")
    s3 = C.s3_client()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=C.BUCKET, Prefix=f"hw4/{a.run}/"):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=C.BUCKET, Delete={"Objects": objs})
    say(f"подготовка: {base} очищен, указатель {base}/result создан (версия "
        f"{zk.exists(base + '/result').version}), FENCING={a.fencing}, пауза после {PAUSE_AT} чанков на {PAUSE_S:g} с")

    env = dict(os.environ, FENCING=str(a.fencing), PYTHONUNBUFFERED="1")
    procs = {}
    for name in ("coord-A", "coord-B"):
        procs[name] = subprocess.Popen([sys.executable, str(HERE / "coordinator.py"), "--name", name, "--run", a.run],
                                       env=env, stdout=open(logdir / f"{name}.stdout", "w"),
                                       stderr=subprocess.STDOUT)
        say(f"запущен {name}, pid {procs[name].pid}")
        time.sleep(1)

    # ждём, пока лидер посчитает PAUSE_AT чанков
    leader = None
    while True:
        data, st = zk.get(f"{base}/epoch")
        done = len(zk.get_children(f"{base}/checkpoint"))
        if data and done >= PAUSE_AT:
            leader = data.decode()
            break
        if any(p.poll() is not None for p in procs.values()):
            raise SystemExit("координатор завершился раньше паузы")
        time.sleep(0.1)
    backup = "coord-B" if leader == "coord-A" else "coord-A"
    t_stop = time.time()
    os.kill(procs[leader].pid, signal.SIGSTOP)
    say(f"SIGSTOP лидеру {leader} (pid {procs[leader].pid}, эпоха {st.version}) после {done} готовых чанков")

    # ждём смены лидера и публикации запасным
    t_new = None
    while time.time() - t_stop < PAUSE_S:
        data, st2 = zk.get(f"{base}/epoch")
        if t_new is None and data.decode() == backup:
            t_new = time.time()
            say(f"ZooKeeper: лидер теперь {backup} (эпоха {st2.version}), "
                f"через {t_new - t_stop:.2f} с после SIGSTOP")
        time.sleep(0.05)
    # лидер стоит, пока запасной не доделает расчёт (не меньше PAUSE_S секунд)
    while procs[backup].poll() is None and time.time() - t_stop < 180:
        time.sleep(0.1)
    rv = zk.exists(f"{base}/result").version
    say(f"пауза {time.time() - t_stop:.1f} с; {backup} завершился с кодом {procs[backup].returncode}, "
        f"версия указателя {rv}")
    os.kill(procs[leader].pid, signal.SIGCONT)
    say(f"SIGCONT лидеру {leader}: процесс продолжает работу с места остановки")

    for name, p in procs.items():
        p.wait(timeout=180)
        say(f"{name} завершился, код {p.returncode}")

    # итог
    data, st = zk.get(f"{base}/result")
    ptr = json.loads(data)
    say(f"указатель {base}/result: версия {st.version}, записал {ptr['by']} (эпоха {ptr['epoch']}), "
        f"{ptr['result']}, пар {ptr['pairs']}, вхождений {ptr['occurrences']}")
    lines = {n: (logdir / f"{n}.log").read_text(encoding="utf-8").splitlines() for n in procs}
    wrote = [(n, l) for n, ls in lines.items() for l in ls if "записал результат" in l]
    bad = [(n, l) for n, ls in lines.items() for l in ls if "BadVersionError" in l]
    say(f"строк «записал результат»: {len(wrote)}; строк BadVersionError: {len(bad)}")
    for n, l in wrote + bad:
        say(f"  {l}")
    t_lead = [parse_ts(l) for l in lines[backup] if l.endswith("стал лидером")][0]
    say(f"смена лидера по журналам: SIGSTOP {datetime.fromtimestamp(t_stop):%H:%M:%S.%f}"[:-3] +
        f" -> {backup} «стал лидером» {datetime.fromtimestamp(t_lead):%H:%M:%S.%f}"[:-3] +
        f", {t_lead - t_stop:.2f} с")
    objs = s3.list_objects_v2(Bucket=C.BUCKET, Prefix=f"hw4/{a.run}/result")["Contents"]
    for o in objs:
        say(f"  s3://{C.BUCKET}/{o['Key']} {o['Size']} байт")
    json.dump({"run": a.run, "fencing": a.fencing, "leader": leader, "backup": backup,
               "failover_s": round(t_lead - t_stop, 3), "pointer_version": st.version,
               "pointer": ptr, "wrote": len(wrote), "bad_version": len(bad)},
              open(logdir / "summary.json", "w"), ensure_ascii=False, indent=1)
    zk.stop()


if __name__ == "__main__":
    main()
