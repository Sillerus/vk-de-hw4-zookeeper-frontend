"""Координатор расчёта пар навыков (ДЗ 3) с выбором лидера через ZooKeeper.

Несколько копий координатора соревнуются за лидерство (рецепт Election из kazoo:
эфемерные последовательные узлы). Лидер:
  1. увеличивает эпоху: set /hw4/<run>/epoch, номер эпохи = версия узла;
  2. читает чекпоинт /hw4/<run>/checkpoint (дети chunk-XXXXX) и продолжает с первого
     несделанного чанка;
  3. для каждого чанка читает свой диапазон байт объекта s3://hh-lake/raw/... (код
     MapReduce из ДЗ 3), считает пары навыков варианта 8, кладёт частичный результат
     в s3://hh-lake/hw4/<run>/partials/ и отмечает чанк в чекпоинте;
  4. сводит частичные результаты в k2_pairs.csv того же формата, что в ДЗ 3,
     сверяет SHA-256 с ДЗ 3 и публикует указатель /hw4/<run>/result.

Публикация указателя:
  FENCING=0  zk.set(result) без условий, кто последний, тот и записал;
  FENCING=1  транзакция check(epoch, version=моя_эпоха) + set(result). Если за время
             паузы лидером стал другой координатор, версия epoch уже больше, и
             транзакция падает с BadVersionError.

Запуск: FENCING=0 python coordinator.py --name coord-A --run nofence
"""
import argparse
import csv
import gzip
import hashlib
import io
import json
import logging
import os
import socket
import statistics
import sys
import time
from datetime import datetime
from itertools import combinations
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
ENV = REPO / ".env"
if ENV.exists():
    for line in ENV.read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            k, v = line.strip().split("=", 1)
            os.environ.setdefault(k, v)
sys.path.insert(0, str(Path(__file__).resolve().parent / "hw3"))

from kazoo.client import KazooClient, KazooState                  # noqa: E402
from kazoo.exceptions import BadVersionError, NodeExistsError  # noqa: E402

from common import BUCKET, s3_client                             # noqa: E402  (код ДЗ 3)
from mapreduce import map_records, read_chunk_lines              # noqa: E402  (код ДЗ 3)

ZK_HOSTS = os.environ.get("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
SESSION_TIMEOUT = float(os.environ.get("SESSION_TIMEOUT", "10"))
FENCING = os.environ.get("FENCING", "0") == "1"
CHUNK_MB = float(os.environ.get("CHUNK_MB", "16"))
RAW_KEY = "raw/it_vacancies_2024.csv"
HW3_PAIRS = "results/mapreduce/chunk64/k2_pairs.csv"   # эталон из ДЗ 3


def setup_log(run, name):
    d = REPO / "logs" / run
    d.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s.%(msecs)03d [%(name)s pid=%(process)d] %(message)s",
                            "%Y-%m-%d %H:%M:%S")
    for h in (logging.FileHandler(d / f"{name}.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)):
        h.setFormatter(fmt)
        log.addHandler(h)
    return log


class Coordinator:
    def __init__(self, name, run):
        self.name, self.run = name, run
        self.base = f"/hw4/{run}"
        self.log = setup_log(run, name)
        self.s3 = s3_client()
        self.zk = KazooClient(hosts=ZK_HOSTS, timeout=SESSION_TIMEOUT)
        self.zk.add_listener(self.on_state)

    # ---------------------------------------------------------------- ZooKeeper
    def on_state(self, state):
        # вызывается из потока kazoo: только пишем в журнал
        msg = {KazooState.SUSPENDED: "связь с ZooKeeper прервана (SUSPENDED)",
               KazooState.LOST: "сессия ZooKeeper потеряна (LOST), эфемерные узлы удалены",
               KazooState.CONNECTED: "подключён к ZooKeeper (CONNECTED)"}[state]
        self.log.info(msg)

    def zk_call(self, fn, *a, **kw):
        return self.zk.retry(fn, *a, **kw)

    # ---------------------------------------------------------------- расчёт
    def chunks(self):
        size = self.s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
        step = int(CHUNK_MB * 1024 * 1024)
        return size, [(i, st, min(st + step, size)) for i, st in enumerate(range(0, size, step))]

    def done_chunks(self):
        return set(self.zk_call(self.zk.get_children, f"{self.base}/checkpoint"))

    def process_chunk(self, cid, start, end, size):
        """Map + combine одного чанка (функции ДЗ 3), только k = 2 и общий scope."""
        data, nbytes = read_chunk_lines(self.s3, start, end, size)
        records, stats = map_records(data, start == 0)
        acc = {}
        for _area, skills, sal in records:
            for pair in combinations(skills, 2):
                v = acc.setdefault(pair, [0, []])
                v[0] += 1
                if sal is not None:
                    v[1].append(sal)
        body = gzip.compress("\n".join(json.dumps([a, b, c, s], ensure_ascii=False)
                                       for (a, b), (c, s) in acc.items()).encode(), 1)
        self.s3.put_object(Bucket=BUCKET, Key=f"hw4/{self.run}/partials/chunk-{cid:05d}.jsonl.gz", Body=body)
        return stats, len(acc), nbytes

    def reduce(self, n_chunks):
        acc = {}
        for cid in range(n_chunks):
            obj = self.s3.get_object(Bucket=BUCKET, Key=f"hw4/{self.run}/partials/chunk-{cid:05d}.jsonl.gz")
            text = gzip.decompress(obj["Body"].read()).decode()
            for line in filter(None, text.split("\n")):
                a, b, c, s = json.loads(line)
                v = acc.setdefault((a, b), [0, []])
                v[0] += c
                v[1].extend(s)
        rows = sorted(((k, c, s) for k, (c, s) in acc.items()), key=lambda r: (-r[1], r[0]))
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(["skill1", "skill2", "vacancies", "n_salary", "median_salary"])
        for (a, b), c, s in rows:
            w.writerow([a, b, c, len(s), f"{statistics.median(s):.2f}" if s else ""])
        return buf.getvalue().encode(), rows

    # ---------------------------------------------------------------- лидер
    def lead(self):
        t0 = time.time()
        self.log.info("стал лидером")
        estat = self.zk_call(self.zk.set, f"{self.base}/epoch", self.name.encode())
        self.epoch = estat.version
        self.log.info(f"эпоха {self.epoch} (версия узла {self.base}/epoch)")
        size, tasks = self.chunks()
        done = self.done_chunks()
        todo = [t for t in tasks if f"chunk-{t[0]:05d}" not in done]
        if done:
            self.log.info(f"чекпоинт: готово {len(done)} из {len(tasks)} чанков, продолжаю с чанка {todo[0][0] if todo else '-'}")
        else:
            self.log.info(f"чекпоинт пуст: начинаю расчёт с нуля, {len(tasks)} чанков по {CHUNK_MB:g} МиБ")
        skipped = []
        for cid, st, en in todo:
            if f"chunk-{cid:05d}" in self.done_chunks():
                skipped.append(cid)       # чанк уже сделал другой координатор
                continue
            if skipped:
                self.log.info(f"чанки {skipped[0]}-{skipped[-1]} ({len(skipped)} шт.) уже в чекпоинте, пропускаю")
                skipped = []
            tc = time.time()
            stats, keys, nbytes = self.process_chunk(cid, st, en, size)
            try:
                self.zk_call(self.zk.create, f"{self.base}/checkpoint/chunk-{cid:05d}",
                             json.dumps({"by": self.name, "epoch": self.epoch}).encode())
            except NodeExistsError:
                self.log.info(f"чанк {cid}: отметка в чекпоинте уже есть")
            n_done = len(self.done_chunks())
            self.log.info(f"чанк {cid} готов ({n_done}/{len(tasks)}): строк {stats['lines']}, "
                          f"вакансий варианта {stats['variant']}, пар {keys}, {time.time() - tc:.2f} с")
        if skipped:
            self.log.info(f"чанки {skipped[0]}-{skipped[-1]} ({len(skipped)} шт.) уже в чекпоинте, пропускаю")
        # reduce и сверка с ДЗ 3
        tr = time.time()
        data, rows = self.reduce(len(tasks))
        sha = hashlib.sha256(data).hexdigest()
        ref = hashlib.sha256(self.s3.get_object(Bucket=BUCKET, Key=HW3_PAIRS)["Body"].read()).hexdigest()
        occ = sum(r[1] for r in rows)
        top = rows[0]
        self.log.info(f"reduce: {time.time() - tr:.2f} с; контрольные числа: уникальных пар {len(rows)}, "
                      f"вхождений {occ}, {top[0][0]} + {top[0][1]} = {top[1]}")
        self.log.info(f"sha256 k2_pairs.csv {sha[:16]}..., ДЗ 3 {ref[:16]}... -> "
                      f"{'СОВПАДАЕТ с ДЗ 3' if sha == ref else 'НЕ СОВПАДАЕТ с ДЗ 3'}")
        key = f"hw4/{self.run}/result-{self.name}-epoch{self.epoch}.csv"
        self.s3.put_object(Bucket=BUCKET, Key=key, Body=data)
        self.publish(key, sha, len(rows), occ)
        self.log.info(f"работа лидера заняла {time.time() - t0:.1f} с, выхожу")

    def publish(self, key, sha, uniq, occ):
        payload = json.dumps({"result": f"s3://{BUCKET}/{key}", "sha256": sha, "pairs": uniq,
                              "occurrences": occ, "by": self.name, "epoch": self.epoch,
                              "at": datetime.now().isoformat(timespec="milliseconds")},
                             ensure_ascii=False).encode()
        path = f"{self.base}/result"
        if not FENCING:
            stat = self.zk_call(self.zk.set, path, payload)
            self.log.info(f"записал результат: {path} -> s3://{BUCKET}/{key}, версия указателя {stat.version}")
            return
        tx = self.zk.transaction()
        tx.check(f"{self.base}/epoch", version=self.epoch)
        tx.set_data(path, payload)
        res = self.zk_call(tx.commit)
        if isinstance(res[0], BadVersionError):
            cur = self.zk_call(self.zk.get, f"{self.base}/epoch")[1].version
            self.log.info(f"BadVersionError: запись результата отклонена, моя эпоха {self.epoch}, "
                          f"текущая эпоха {cur}; вторая операция транзакции (set указателя) не применена: "
                          f"{type(res[1]).__name__}")
            return
        self.log.info(f"записал результат: {path} -> s3://{BUCKET}/{key}, версия указателя {res[1].version} "
                      f"(проверка эпохи {self.epoch} пройдена)")

    def main(self):
        self.log.info(f"старт: host={socket.gethostname()} ZK={ZK_HOSTS} session_timeout={SESSION_TIMEOUT:g}s "
                      f"FENCING={int(FENCING)} run={self.run}")
        self.zk.start(timeout=15)
        self.log.info(f"сессия 0x{self.zk.client_id[0]:x}, согласованный тайм-аут "
                      f"{self.zk._session_timeout / 1000:g} с")
        for p in ("checkpoint", "election"):
            self.zk.ensure_path(f"{self.base}/{p}")
        self.log.info("жду лидерства (рецепт Election, эфемерный последовательный узел)")
        self.zk.Election(f"{self.base}/election", self.name).run(self.lead)
        self.zk.remove_listener(self.on_state)
        self.log.info("закрываю сессию ZooKeeper")
        self.zk.stop()
        self.zk.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True)
    ap.add_argument("--run", required=True)
    a = ap.parse_args()
    Coordinator(a.name, a.run).main()
