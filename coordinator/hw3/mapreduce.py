"""Собственная реализация MapReduce поверх объектного хранилища RustFS (S3 API).

Схема:
  driver  -> HEAD объекта, нарезка на чанки по байтам (Range GET), очередь map-задач
  map     -> читает свой диапазон байт из S3, выравнивает границы по '\\n',
             фильтрует вакансии варианта, строит наборы навыков размера k=1,2,3,
             локально агрегирует (combiner) и пишет R партиций в S3 + маркер _SUCCESS
  shuffle -> партиция r каждого маппера достаётся редьюсеру r (hash(key) % R)
  reduce  -> суммирует счётчики и сливает списки зарплат, считает медианы
  driver  -> собирает итоговые таблицы и кладёт их в s3://hh-lake/results/<run>/

Каждая попытка map-задачи запускается отдельным процессом. Если процесс упал
(исключение, kill, os._exit), driver видит ненулевой exitcode или отсутствие
_SUCCESS и перезапускает задачу. Частичные файлы упавшей попытки лежат в своём
каталоге attempt-N и редьюсерами не читаются, поэтому результат не задваивается.

Запуск:
  python mapreduce.py --chunk-mb 64 --workers 2 --reducers 4 --run chunk64
  python mapreduce.py --chunk-mb 64 --fail-map 3 --fail-mode kill --run fail_test
"""
import argparse
import csv
import gzip
import io
import json
import multiprocessing as mp
import os
import statistics
import sys
import time
import zlib
from collections import defaultdict
from itertools import combinations

from common import (BUCKET, MR_TMP_PREFIX, RAW_KEY, REGIONS, RESULTS_PREFIX,
                    VARIANT_ROLE, is_variant, normalize_skills, s3_client,
                    salary_net)

KS = (1, 2, 3)
ALL = "ALL"
# индексы нужных колонок CSV
COL = dict(area=5, name=10, role=11, salary_from=13, salary_to=14,
           currency=15, gross=16, key_skills=20)
ROLE_BYTES = VARIANT_ROLE.encode("utf-8")
TAIL_STEP = 256 * 1024


# ---------------------------------------------------------------- чтение чанка
def read_chunk_lines(s3, start, end, size):
    """Вернуть полные строки, которые НАЧИНАЮТСЯ в диапазоне [start, end).

    Читаем на 1 байт раньше start: если перед start стоит '\\n', то первая
    строка наша целиком, иначе её хвост принадлежит предыдущему чанку.
    Последнюю строку дочитываем за пределами end до ближайшего '\\n'."""
    lo = max(start - 1, 0)
    hi = end - 1
    data = s3.get_object(Bucket=BUCKET, Key=RAW_KEY,
                         Range=f"bytes={lo}-{hi}")["Body"].read()
    nbytes = len(data)
    if start > 0:
        if data[:1] == b"\n":
            data = data[1:]
        else:
            cut = data.find(b"\n")
            data = b"" if cut < 0 else data[cut + 1:]
    # дочитываем хвост последней строки
    pos = end
    while not data.endswith(b"\n") and pos < size and data:
        extra = s3.get_object(Bucket=BUCKET, Key=RAW_KEY,
                              Range=f"bytes={pos}-{min(pos + TAIL_STEP, size) - 1}")["Body"].read()
        nbytes += len(extra)
        cut = extra.find(b"\n")
        if cut >= 0:
            data += extra[:cut + 1]
            break
        data += extra
        pos += len(extra)
    return data, nbytes


def iter_chunk_lines(s3, start, end, size, block=1 << 20, stats=None):
    """Потоковый вариант read_chunk_lines (бонус): тело ответа читается блоками
    по 1 МиБ, строки отдаются сразу, весь чанк в памяти не держится.
    Правило границ то же: строка принадлежит чанку, в котором она начинается."""
    lo = max(start - 1, 0)
    body = s3.get_object(Bucket=BUCKET, Key=RAW_KEY, Range=f"bytes={lo}-{end - 1}")["Body"]
    skipping = start > 0          # ищем первый '\n': до него хвост чужой строки
    buf = b""
    nbytes = 0
    for piece in body.iter_chunks(block):
        nbytes += len(piece)
        if skipping:
            cut = piece.find(b"\n")
            if cut < 0:
                continue
            piece = piece[cut + 1:]
            skipping = False
        buf += piece
        lines = buf.split(b"\n")
        buf = lines.pop()
        yield from lines
    pos = end
    while buf and pos < size:     # последняя строка уходит за end
        extra = s3.get_object(Bucket=BUCKET, Key=RAW_KEY,
                              Range=f"bytes={pos}-{min(pos + TAIL_STEP, size) - 1}")["Body"].read()
        nbytes += len(extra)
        cut = extra.find(b"\n")
        if cut >= 0:
            buf += extra[:cut]
            break
        buf += extra
        pos += len(extra)
    if buf:
        yield buf
    if stats is not None:
        stats["bytes_read"] = nbytes


# ---------------------------------------------------------------- map
def map_records(data, is_first_chunk, prefilter=True):
    """Список (area, skills, salary) по вакансиям варианта.
    data: bytes всего чанка или итератор строк (потоковый режим)."""
    lines = data.split(b"\n") if isinstance(data, bytes) else data
    if is_first_chunk:
        lines = iter(lines)
        next(lines, None)                      # заголовок CSV
    stats = dict(lines=0, parsed=0, variant=0)
    out = []
    for line in lines:
        if not line:
            continue
        stats["lines"] += 1
        if prefilter and ROLE_BYTES not in line:
            continue                           # дешёвый фильтр по байтам до csv
        row = next(csv.reader([line.decode("utf-8")]))
        stats["parsed"] += 1
        if not is_variant(row[COL["role"]], row[COL["name"]]):
            continue
        stats["variant"] += 1
        out.append((row[COL["area"]],
                    normalize_skills(row[COL["key_skills"]]),
                    salary_net(row[COL["salary_from"]], row[COL["salary_to"]],
                               row[COL["currency"]], row[COL["gross"]])))
    return out, stats


def emit(records, regions=True):
    """Combiner: key=(scope, k, combo) -> [count, [salaries]]."""
    acc = {}
    scope_stats = defaultdict(lambda: dict(vacancies=0, with_skills=0, sal=[]))
    for area, skills, sal in records:
        scopes = [ALL] + ([area] if regions and area in REGIONS else [])
        for sc in scopes:
            st = scope_stats[sc]
            st["vacancies"] += 1
            st["with_skills"] += 1 if skills else 0
            if sal is not None:
                st["sal"].append(sal)
            for k in KS:
                for combo in combinations(skills, k):
                    key = (sc, k, combo)
                    v = acc.get(key)
                    if v is None:
                        v = acc[key] = [0, []]
                    v[0] += 1
                    if sal is not None:
                        v[1].append(sal)
    return acc, dict(scope_stats)


def partition(key, R):
    sc, k, combo = key
    return zlib.crc32(f"{sc}\x1f{k}\x1f{chr(31).join(combo)}".encode()) % R


def map_task(run, task_id, start, end, size, R, attempt, fail, opts=None):
    """Тело процесса-маппера. fail = (task_id, mode) для теста отказа.
    opts: prefilter (байтовый фильтр до csv), combiner (локальная агрегация)."""
    opts = opts or dict(prefilter=True, combiner=True, stream=False)
    t0 = time.perf_counter()
    s3 = s3_client()
    base = f"{MR_TMP_PREFIX}/{run}/map-{task_id:05d}/attempt-{attempt}"
    if opts.get("stream"):
        rs = {}
        records, stats = map_records(iter_chunk_lines(s3, start, end, size, stats=rs),
                                     start == 0, prefilter=opts["prefilter"])
        nbytes = rs["bytes_read"]
    else:
        data, nbytes = read_chunk_lines(s3, start, end, size)
        records, stats = map_records(data, start == 0, prefilter=opts["prefilter"])
        del data
    acc, scope_stats = emit(records)
    parts = [[] for _ in range(R)]
    if opts["combiner"]:
        for key, (cnt, sals) in acc.items():
            parts[partition(key, R)].append([key[0], key[1], list(key[2]), cnt, sals])
    else:
        # без combiner: каждое вхождение сочетания уходит в shuffle отдельной записью
        acc = {}
        for area, skills, sal in records:
            for sc in [ALL] + ([area] if area in REGIONS else []):
                for k in KS:
                    for combo in combinations(skills, k):
                        key = (sc, k, combo)
                        parts[partition(key, R)].append([sc, k, list(combo), 1, [] if sal is None else [sal]])
                        acc[key] = 1
    shuffle_bytes = 0
    shuffle_records = sum(len(x) for x in parts)
    for r in range(R):
        if fail and fail[0] == task_id and attempt == 1 and r == R // 2:
            # имитация отказа посреди записи: половина партиций уже лежит в S3
            if fail[1] == "kill":
                os._exit(137)
            raise RuntimeError(f"injected failure in map {task_id}")
        body = gzip.compress("\n".join(json.dumps(x, ensure_ascii=False)
                                       for x in parts[r]).encode(), 1)
        shuffle_bytes += len(body)
        s3.put_object(Bucket=BUCKET, Key=f"{base}/part-{r:03d}.jsonl.gz", Body=body)
    stats.update(task=task_id, attempt=attempt, start=start, end=end,
                 bytes_read=nbytes, keys=len(acc),
                 shuffle_bytes=shuffle_bytes, shuffle_records=shuffle_records,
                 scope_stats={k: dict(vacancies=v["vacancies"], with_skills=v["with_skills"],
                                      sal=v["sal"]) for k, v in scope_stats.items()},
                 seconds=round(time.perf_counter() - t0, 3),
                 maxrss_mb=round(__import__("resource").getrusage(0).ru_maxrss / 1024, 1))
    # коммит: маркер пишется последним
    s3.put_object(Bucket=BUCKET, Key=f"{base}/_SUCCESS", Body=json.dumps(stats).encode())


# ---------------------------------------------------------------- reduce
def reduce_task(args):
    run, r, committed = args
    s3 = s3_client()
    acc = {}
    for prefix in committed:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{prefix}/part-{r:03d}.jsonl.gz")
        text = gzip.decompress(obj["Body"].read()).decode()
        if not text:
            continue
        for line in text.split("\n"):
            sc, k, combo, cnt, sals = json.loads(line)
            key = (sc, k, tuple(combo))
            v = acc.get(key)
            if v is None:
                acc[key] = [cnt, sals]
            else:
                v[0] += cnt
                v[1].extend(sals)
    rows = []
    for (sc, k, combo), (cnt, sals) in acc.items():
        med = round(statistics.median(sals), 2) if sals else None
        rows.append((sc, k, combo, cnt, len(sals), med))
    return rows


# ---------------------------------------------------------------- driver
def run_maps(run, tasks, size, R, workers, fail, max_attempts=3, opts=None):
    """Простой планировщик: не больше workers процессов, перезапуск упавших."""
    s3 = s3_client()
    ctx = mp.get_context("fork")
    pending = [(t, 1) for t in tasks]
    running = {}
    committed = {}
    log = []
    while pending or running:
        while pending and len(running) < workers:
            (tid, st, en), att = pending.pop(0)
            p = ctx.Process(target=map_task, args=(run, tid, st, en, size, R, att, fail, opts))
            p.start()
            running[p] = (tid, st, en, att)
        time.sleep(0.05)
        for p in list(running):
            if p.is_alive():
                continue
            p.join()
            tid, st, en, att = running.pop(p)
            prefix = f"{MR_TMP_PREFIX}/{run}/map-{tid:05d}/attempt-{att}"
            ok = False
            if p.exitcode == 0:
                try:
                    meta = json.loads(s3.get_object(Bucket=BUCKET, Key=f"{prefix}/_SUCCESS")["Body"].read())
                    ok = True
                except s3.exceptions.NoSuchKey:
                    meta = None
            if ok:
                committed[tid] = (prefix, meta)
                log.append(f"map {tid:3d} attempt {att}: OK exit=0 "
                           f"bytes={meta['bytes_read']} lines={meta['lines']} variant={meta['variant']} "
                           f"keys={meta['keys']} {meta['seconds']}s")
            else:
                log.append(f"map {tid:3d} attempt {att}: FAILED exit={p.exitcode} -> retry")
                print(log[-1], flush=True)
                if att >= max_attempts:
                    raise SystemExit(f"map {tid} failed {att} times")
                pending.append(((tid, st, en), att + 1))
    return committed, log


def write_csv(s3, key, header, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    w.writerows(rows)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue().encode())
    return buf.getvalue()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-mb", type=float, default=64)
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--reducers", type=int, default=4)
    ap.add_argument("--run", default=None)
    ap.add_argument("--fail-map", type=int, default=None)
    ap.add_argument("--fail-mode", choices=["exception", "kill"], default="kill")
    ap.add_argument("--keep-tmp", action="store_true")
    ap.add_argument("--no-prefilter", action="store_true", help="парсить csv у каждой строки")
    ap.add_argument("--no-combiner", action="store_true", help="не агрегировать в маппере")
    ap.add_argument("--stream", action="store_true", help="потоковое чтение чанка блоками по 1 МиБ")
    a = ap.parse_args()
    run = a.run or f"chunk{int(a.chunk_mb)}"
    t0 = time.perf_counter()
    s3 = s3_client()
    # чистим старые промежуточные файлы этого запуска
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{MR_TMP_PREFIX}/{run}/"):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})
    size = s3.head_object(Bucket=BUCKET, Key=RAW_KEY)["ContentLength"]
    chunk = int(a.chunk_mb * 1024 * 1024)
    tasks = [(i, st, min(st + chunk, size)) for i, st in enumerate(range(0, size, chunk))]
    print(f"object s3://{BUCKET}/{RAW_KEY} size={size} chunk={chunk} maps={len(tasks)} "
          f"reducers={a.reducers} workers={a.workers}", flush=True)
    fail = (a.fail_map, a.fail_mode) if a.fail_map is not None else None
    tm = time.perf_counter()
    opts = dict(prefilter=not a.no_prefilter, combiner=not a.no_combiner, stream=a.stream)
    committed, log = run_maps(run, tasks, size, a.reducers, a.workers, fail, opts=opts)
    t_map = time.perf_counter() - tm
    n_fail = sum("FAILED" in l for l in log)
    # сводка по мапперам
    total = dict(lines=0, parsed=0, variant=0, bytes_read=0, shuffle_records=0, shuffle_bytes=0)
    scope = defaultdict(lambda: dict(vacancies=0, with_skills=0, sal=[]))
    for tid in sorted(committed):
        meta = committed[tid][1]
        for k in total:
            total[k] += meta[k]
        for sc, v in meta["scope_stats"].items():
            scope[sc]["vacancies"] += v["vacancies"]
            scope[sc]["with_skills"] += v["with_skills"]
            scope[sc]["sal"].extend(v["sal"])
    print(f"map phase: {t_map:.2f}s, committed={len(committed)}/{len(tasks)}, failed attempts={n_fail}")
    print(f"rows scanned={total['lines']} csv-parsed={total['parsed']} variant={total['variant']} "
          f"bytes read={total['bytes_read']} (overhead {total['bytes_read'] - size} bytes)")
    print(f"shuffle: records={total['shuffle_records']} gz bytes={total['shuffle_bytes']}")
    print(f"mapper peak RSS: max={max(committed[t][1]['maxrss_mb'] for t in committed):.0f} MB")
    tr = time.perf_counter()
    prefixes = [committed[t][0] for t in sorted(committed)]
    with mp.get_context("fork").Pool(min(a.workers, a.reducers)) as pool:
        parts = pool.map(reduce_task, [(run, r, prefixes) for r in range(a.reducers)])
    rows = [x for p in parts for x in p]
    t_red = time.perf_counter() - tr
    print(f"reduce phase: {t_red:.2f}s, keys={len(rows)}")
    # итоговые таблицы
    out = f"{RESULTS_PREFIX}/mapreduce/{run}"
    names = {1: "k1_skills", 2: "k2_pairs", 3: "k3_triples"}
    for k in KS:
        sel = sorted([r for r in rows if r[0] == ALL and r[1] == k], key=lambda r: (-r[3], r[2]))
        header = [f"skill{i + 1}" for i in range(k)] + ["vacancies", "n_salary", "median_salary"]
        write_csv(s3, f"{out}/{names[k]}.csv", header,
                  [list(r[2]) + [r[3], r[4], "" if r[5] is None else f"{r[5]:.2f}"] for r in sel])
        occ = sum(r[3] for r in sel)
        print(f"k={k}: unique={len(sel)} occurrences={occ} top3={[(' + '.join(r[2]), r[3]) for r in sel[:3]]}")
    reg = sorted([r for r in rows if r[0] != ALL], key=lambda r: (r[0], r[1], -r[3], " + ".join(r[2])))
    write_csv(s3, f"{out}/regions_combos.csv",
              ["region", "k", "combo", "vacancies", "n_salary", "median_salary"],
              [[r[0], r[1], " + ".join(r[2]), r[3], r[4], "" if r[5] is None else f"{r[5]:.2f}"] for r in reg])
    summ = []
    for sc in [ALL] + REGIONS:
        v = scope[sc]
        med = round(statistics.median(v["sal"]), 2) if v["sal"] else None
        summ.append([sc, v["vacancies"], v["with_skills"], len(v["sal"]), f"{med:.2f}"])
    write_csv(s3, f"{out}/scope_summary.csv",
              ["scope", "vacancies", "with_skills", "n_salary", "median_salary"], summ)
    for r in summ:
        print("summary", r)
    s3.put_object(Bucket=BUCKET, Key=f"{out}/_job.json", Body=json.dumps(dict(
        run=run, chunk_bytes=chunk, maps=len(tasks), reducers=a.reducers, workers=a.workers,
        failed_attempts=n_fail, map_log=log, t_map=t_map, t_reduce=t_red,
        total_seconds=time.perf_counter() - t0), ensure_ascii=False, indent=1).encode())
    if not a.keep_tmp:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"{MR_TMP_PREFIX}/{run}/"):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})
    import resource
    print(f"driver peak RSS: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024:.0f} MB, "
          f"reducers peak RSS: {resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024:.0f} MB (max over children)")
    print(f"results -> s3://{BUCKET}/{out}/  total {time.perf_counter() - t0:.2f}s")


if __name__ == "__main__":
    main()
