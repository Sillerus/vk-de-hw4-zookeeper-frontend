"""Общие настройки: клиент S3 (RustFS), имена объектов, условие варианта."""
import os
import re
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]


def _load_env():
    env_file = ROOT / "infra" / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k, v)


_load_env()

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000")
BUCKET = os.environ.get("S3_BUCKET", "hh-lake")
ACCESS_KEY = os.environ["RUSTFS_ACCESS_KEY"]
SECRET_KEY = os.environ["RUSTFS_SECRET_KEY"]

RAW_KEY = "raw/it_vacancies_2024.csv"          # исходный объект в бакете
RESULTS_PREFIX = "results"                      # итоговые таблицы
MR_TMP_PREFIX = "mr-tmp"                        # промежуточные файлы MapReduce

# Вариант 8: "Программист, разработчик" + Frontend.
VARIANT_ROLE = "Программист, разработчик"
# Frontend в названии: front-end, front - end, front\u2013end (короткое тире U+2013),
# frontеnd (кириллическая е), опечатки
# fronted/frontent, фронтенд/фронтэнд, а также "Front-разработчик", "Фронт разработчик".
# Простое "front|фронт" не подходит: ловит "фронт-офис", "фронтальная система".
FRONT_RE = (r"front\s*[-\u2013]?\s*[eе]n[dt]|fronted|фронт\s*[-\u2013]?\s*[еэ]нд"
            r"|(front|фронт)\s*[-\u2013]?\s*(разработчик|developer|программист)")
# Fullstack-вакансии исключаем: это отдельный профиль (frontend + backend)
FULL_RE = r"full[\s\-]?stack|фул+[\s\-]?ст[еэ]к"
_front = re.compile(FRONT_RE)
_full = re.compile(FULL_RE)


def is_variant(role: str, name: str) -> bool:
    if role != VARIANT_ROLE:
        return False
    n = (name or "").lower()
    return bool(_front.search(n)) and not _full.search(n)


_ws = re.compile(r"[ \t\n\r\f\v]+")


def normalize_skills(raw: str):
    """'React; TypeScript;  react ' -> ['react', 'typescript'].

    Правило совпадает с SQL в DuckDB: split по ';', схлопывание ASCII-пробелов,
    trim, lower, удаление пустых и дублей, сортировка по коду символа."""
    if not raw:
        return []
    out = set()
    for s in raw.split(";"):
        s = _ws.sub(" ", s).strip(" ").lower()
        if s:
            out.add(s)
    return sorted(out)


def salary_net(salary_from: str, salary_to: str, currency: str, gross: str):
    """Зарплата вакансии в рублях на руки: середина вилки или одна граница.

    gross=true -> умножаем на 0.87 (НДФЛ 13%). Не RUR и выбросы вне
    [10 000; 1 500 000] отбрасываем. Порядок операций такой же, как в SQL."""
    if currency != "RUR":
        return None
    f = float(salary_from) if salary_from else None
    t = float(salary_to) if salary_to else None
    if f is not None and t is not None:
        s = (f + t) / 2
    elif f is not None:
        s = f
    elif t is not None:
        s = t
    else:
        return None
    if gross == "true":
        s = s * 0.87
    if s < 10000 or s > 1500000:
        return None
    return s


REGIONS = ["Москва", "Санкт-Петербург", "Екатеринбург"]


def s3_client(max_attempts: int = 3):
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"},
                      retries={"max_attempts": max_attempts},
                      max_pool_connections=32),
    )
