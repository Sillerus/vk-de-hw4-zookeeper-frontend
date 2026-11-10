"""Снимки терминала для отчёта: фрагменты берутся из реальных журналов в ../logs
и отрисовываются в окне терминала через Chromium (Playwright)."""
import asyncio
import html
import re
from pathlib import Path

from playwright.async_api import async_playwright

HERE = Path(__file__).parent
LOGS = HERE.parent / "logs"
IMG = HERE / "img"
IMG.mkdir(exist_ok=True)
MARK = [r"Mode: leader", r"Mode: follower", r"Error contacting service[^\n]*", r"not currently serving requests",
        r"ConnectionLoss[^\n]*", r"записал результат", r"BadVersionError", r"dataVersion = \d",
        r"ИСЧЕЗ", r"СОВПАДАЕТ с ДЗ 3", r"стал лидером", r"SIGSTOP", r"SIGCONT", r"LOST", r"\d+\.\d+ с после SIGSTOP"]


def read(p):
    return (LOGS / p).read_text(encoding="utf-8")


def between(text, start, end=None):
    i = text.index(start)
    j = text.index(end, i + len(start)) if end else len(text)
    return text[i:j].rstrip()


def render(text):
    out = html.escape(text)
    for m in MARK:
        out = re.sub(m, lambda x: f"<b>{x.group(0)}</b>", out)
    out = re.sub(r"^(\[[\d:.]+\] (?:окно \d )?\$ .*|\$ .*)$", r"<span class=cmd>\1</span>", out, flags=re.M)
    return out


PAGE = """<!doctype html><html><head><meta charset=utf-8><style>
body{margin:0;background:#d9dce1;padding:18px;font-family:'DejaVu Sans Mono',monospace}
.win{width:1240px;border-radius:8px;overflow:hidden;box-shadow:0 6px 24px rgba(0,0,0,.25)}
.bar{background:#3b3f46;color:#cfd3da;font:13px 'DejaVu Sans',sans-serif;padding:7px 12px;display:flex;gap:7px;align-items:center}
.dot{width:12px;height:12px;border-radius:50%;display:inline-block}
pre{margin:0;background:#1e2127;color:#d7dae0;font-size:12.5px;line-height:1.45;padding:12px 14px;white-space:pre-wrap;word-break:break-all}
b{color:#7ee787;font-weight:bold} .cmd{color:#79c0ff}
</style></head><body><div class=win><div class=bar><span class=dot style="background:#ff5f57"></span>
<span class=dot style="background:#febc2e"></span><span class=dot style="background:#28c840"></span>
&nbsp;TITLE</div><pre>BODY</pre></div></body></html>"""

s2 = read("stage2_ensemble.txt")
eph = read("stage2_ephemeral.txt")
SHOTS = {
    "t1_roles": ("root@vm: ~/hw4 - запуск ансамбля и роли серверов",
                 between(s2, "=== 1.", "] $ for p").rsplit("\n[", 1)[0]),
    "t2_stop_one": ("root@vm: ~/hw4 - остановлен один сервер (лидер)",
                    between(s2, "=== 2.", "=== 3.")),
    "t3_stop_two": ("root@vm: ~/hw4 - остановлены два сервера из трёх",
                    between(s2, "=== 3.", "=== 4.")),
    "t4_restart": ("root@vm: ~/hw4 - оба сервера запущены снова", between(s2, "=== 4.")),
    "t5_ephemeral": ("root@vm: ~/hw4 - эфемерный узел: окно 2 закрыто", between(eph, "=== Опыт Б")),
    "t6_nofence": ("root@vm: ~/hw4/coordinator - этап 3, FENCING=0", read("nofence/scenario.log")),
    "t7_fencing": ("root@vm: ~/hw4/coordinator - этап 4, FENCING=1", read("fencing/scenario.log")),
    "t8_pointer": ("root@vm: ~/hw4/coordinator - указатель результата в zkCli",
                   between(read("nofence/zkcli_pointer.txt"), "$", "\n$ zkCli.sh -server $ZK_ALL get -s /hw4/nofence/epoch")
                   + "\n\n" + between(read("fencing/zkcli_pointer.txt"), "$", "\n$ zkCli.sh -server $ZK_ALL get -s /hw4/fencing/epoch")),
}

async def main():
    async with async_playwright() as p:
        b = await p.chromium.launch(executable_path="/opt/pw-browsers/chromium-1194/chrome-linux/chrome")
        pg = await b.new_page(viewport={"width": 1276, "height": 400}, device_scale_factor=2)
        for name, (title, text) in SHOTS.items():
            await pg.set_content(PAGE.replace("TITLE", html.escape(title)).replace("BODY", render(text)))
            el = await pg.query_selector(".win")
            await el.screenshot(path=str(IMG / f"{name}.png"))
            print(name)
        await b.close()

asyncio.run(main())
