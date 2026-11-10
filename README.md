# ДЗ 4. Обеспечение отказоустойчивости средствами Apache ZooKeeper

Инженерия данных, VK Education. Московкин Александр Николаевич, ИСУ 472264, группа J3312.
Вариант 8: «Программист, разработчик», Frontend.

**Отчёт:** [`report/report.pdf`](report/report.pdf)

## Что сделано

| Этап | Где | Результат |
|---|---|---|
| 1. Установка | `zk/setup_ensemble.sh`, `zk/env.sh` | Java 17, три копии ZooKeeper 3.9.6 в `/opt/hw4-zk/zk1..zk3` |
| 2. Ансамбль | `zk/conf/zoo{1,2,3}.cfg`, `zk/stage2_ensemble.sh` | роли leader / follower; без одного сервера работает, без двух `ConnectionLoss` |
| 2. Эфемерный узел | `zk/ephemeral_demo.py` | исчезает сразу после `quit` и через 9,8 с после закрытия окна |
| 3. Без защиты | `coordinator/`, `logs/nofence/` | две строки «записал результат», указатель `dataVersion = 2` |
| 4. С защитой | `FENCING=1`, `logs/fencing/` | `BadVersionError`, указатель `dataVersion = 1` от coord-B |
| Смена лидера | `logs/*/summary.json` | 10,47-11,85 с, среднее 11,37 с при тайм-ауте сессии 10 с |
| Сверка с ДЗ 3 | `coordinator/coordinator.py` | 23 204 пары, 121 358 вхождений, SHA-256 `k2_pairs.csv` = ДЗ 3 |


## Запуск

```bash
sudo apt install openjdk-17-jdk-headless netcat-openbsd
pip install -r requirements.txt
cp .env.example .env        # ключи RustFS; в бакете hh-lake должны быть данные и результаты ДЗ 3
./run_all.sh                # этапы 1-4, около 5 минут, все выводы в logs/
```

## Журналы

- `logs/stage1_setup.txt`, `logs/stage2_ensemble.txt`, `logs/stage2_ephemeral.txt` - выводы команд этапов 1-2
- `logs/zk-server/zk{1,2,3}.log` - журналы серверов ZooKeeper (выборы лидера, LOOKING)
- `logs/nofence/`, `logs/fencing/` - журналы координаторов `coord-A.log`, `coord-B.log`, сценарий `scenario.log`, указатель в zkCli `zkcli_pointer.txt`, итог `summary.json`
- `logs/failover1..3/` - повторные замеры времени смены лидера
