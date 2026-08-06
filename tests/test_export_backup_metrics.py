"""
Контракт на `scripts/export-backup-metrics.sh`: экспортируемое число байт обязано
быть РАВНО измеренному, а не «примерно ему».

Заведён 2026-08-06 после регрессии, которую не поймал ни один из существующих
тестов, потому что все они статические.

История класса — две редакции одной строки, обе сломанные по-разному:

  1. `awk '{print $2+0}'` — арифметика awk идёт через double и печатается
     форматом `%.6g`: 10366531632 превращалось в `1.03665e+10`.
  2. `awk '{printf "%d", $2}'` — на сервере awk это mawk 1.3.4, а он печатает
     `%d` через int32 и НАСЫЩАЕТ: 10717881069 становилось 2147483647
     (ровно INT32_MAX).

Вторая редакция опаснее первой: `1.03665e+10` в textfile-коллекторе выглядит
как поломка, а 2147483647 — как честное измерение. Замер 06.08.2026 на живом
Prometheus: `min_over_time(frontier_s3_bucket_bytes[7d])` = 2147483647,
`max_over_time(...)` = 14161838172. То есть ряд ходил между настоящим значением
(сразу после опроса S3) и потолком int32 (всё остальное время, когда значение
бралось из кэша). Оба зависящих правила при этом не могли сработать никогда:
`FrontierS3QuotaHigh` требует ratio > 0.85 держащийся `for: 1h`, а насыщенное
значение даёт 0.133; за это окно бакет реально доходил до 13.2 ГиБ — 88% квоты.

Поэтому проверка здесь ПОВЕДЕНЧЕСКАЯ: скрипт запускается на подставном
FRONTIER_ROOT и его вывод сверяется с содержимым кэша побайтно. Статическая
проверка «в файле нет printf %d» прошла бы зелёной на первой редакции.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "export-backup-metrics.sh"

# Больше INT32_MAX (2147483647) и с ненулевыми младшими разрядами: число обязано
# пережить и обрезание до int32, и потерю точности в %.6g.
BUCKET_BYTES = "10717881069"
CACHE_EPOCH = "1786036021"


# На Windows `shutil.which("bash")` первым отдаёт bash из WSL, а он не видит ни
# путей вида D:\..., ни каталога pytest tmp_path — тест падал бы на окружении,
# а не на дефекте. Поэтому кандидатов перебираем и берём того, кто ДЕЙСТВИТЕЛЬНО
# видит скрипт по его настоящему пути. На Linux первый же кандидат подходит.
_BASH_CANDIDATES: tuple[str, ...] = (
    r"C:\Program Files\Git\bin\bash.exe",
    r"C:\Program Files\Git\usr\bin\bash.exe",
)


def _bash() -> str:
    candidates = [c for c in (shutil.which("bash"), *_BASH_CANDIDATES) if c and Path(c).exists()]
    for candidate in candidates:
        probe = subprocess.run(  # noqa: S603 — фиксированный набор кандидатов
            [candidate, "-c", f'test -f "{SCRIPT}"'],
            capture_output=True,
            timeout=60,
        )
        if probe.returncode == 0:
            return candidate
    pytest.skip("нет bash, который видит путь репозитория (на Windows это WSL-only окружение)")


def _run_exporter(root: Path, cache_line: str) -> str:
    (root / "runtime").mkdir(parents=True, exist_ok=True)
    # newline="" — иначе на Windows Python подменит \n на \r\n и тест будет
    # проверять устойчивость к CRLF вместо разрядности числа.
    (root / "runtime" / "s3-usage-cache").write_text(cache_line, encoding="utf-8", newline="")

    env = dict(os.environ)
    env["FRONTIER_ROOT"] = str(root)
    # Опрос S3 требует docker и сети. Ставим интервал заведомо больше возраста
    # кэша, чтобы ветка свежего опроса не выполнялась и тест остался герметичным.
    env["S3_POLL_MIN_INTERVAL"] = "999999999"

    proc = subprocess.run(  # noqa: S603 — фиксированный путь к скрипту репозитория
        [_bash(), str(SCRIPT)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, f"скрипт упал: {proc.stderr}"
    return (root / "prometheus" / "textfile" / "frontier_backup.prom").read_text(encoding="utf-8")


def _sample(text: str, name: str) -> str:
    for line in text.splitlines():
        if line.startswith(f"{name} "):
            return line.split(" ", 1)[1].strip()
    raise AssertionError(f"в экспозиции нет серии {name}:\n{text}")


def test_bucket_bytes_survives_the_trip_through_the_cache(tmp_path: Path) -> None:
    """Число из кэша обязано выйти из экспортёра тем же числом."""
    now_line = f"{CACHE_EPOCH} {BUCKET_BYTES}\n"
    text = _run_exporter(tmp_path, now_line)

    exported = _sample(text, "frontier_s3_bucket_bytes")

    assert exported == BUCKET_BYTES, (
        f"экспортировано {exported!r} вместо {BUCKET_BYTES!r}. "
        "Так выглядит и насыщение int32 (2147483647), и потеря точности в %.6g."
    )
    assert _sample(text, "frontier_s3_bucket_measured_timestamp_seconds") == CACHE_EPOCH


def test_bucket_bytes_is_not_clamped_to_int32(tmp_path: Path) -> None:
    """Отдельный кейс на сам потолок: он должен ломать тест громко и по имени."""
    text = _run_exporter(tmp_path, f"{CACHE_EPOCH} {BUCKET_BYTES}\n")
    exported = _sample(text, "frontier_s3_bucket_bytes")

    assert exported != "2147483647", (
        "значение зажато в INT32_MAX — вернулась регрессия mawk %d, "
        "из-за которой FrontierS3QuotaHigh не могла сработать ни при каком наполнении"
    )
    assert "e+" not in exported.lower(), (
        "значение ушло в научную нотацию — вернулась регрессия awk %.6g"
    )


def test_cache_is_not_read_through_awk_numeric_formats() -> None:
    """Страховка, не зависящая от того, какой awk стоит на хосте прогона.

    Поведенческие кейсы выше поймают насыщение только там, где awk — mawk
    (сервер). На хосте с gawk `printf "%d"` отработает верно, и зелёный тест
    соврал бы про исправность. Поэтому способ чтения кэша зафиксирован
    структурно: поля берутся встроенным `read`, а не числовым форматом awk.
    """
    text = SCRIPT.read_text(encoding="utf-8")

    assert 'read -r S3_TS S3_BYTES' in text, (
        "кэш занятости бакета обязан читаться встроенным `read`: любой числовой "
        "формат awk уже дважды терял разряды (%.6g) или насыщался в int32 (%d)"
    )
    cache_lines = [ln for ln in text.splitlines() if "$CACHE" in ln and not ln.lstrip().startswith("#")]
    assert cache_lines, "в скрипте не осталось ни одной строки, работающей с кэшем"
    assert not any("awk" in ln for ln in cache_lines), (
        f"кэш снова читается через awk: {cache_lines}"
    )


def test_broken_cache_line_becomes_an_honest_zero(tmp_path: Path) -> None:
    """Мусор в кэше обязан дать 0, а не остаток от разбора и не падение скрипта.

    Ноль здесь честен: `frontier_s3_quota_bytes > 0` в обоих правилах отсекает
    ненастроенную квоту, а нулевая занятость при заданной квоте видна на дашборде
    рядом с `frontier_s3_bucket_measured_timestamp_seconds` = 0.
    """
    text = _run_exporter(tmp_path, "мусор без чисел\n")

    assert _sample(text, "frontier_s3_bucket_bytes") == "0"
    assert _sample(text, "frontier_s3_bucket_measured_timestamp_seconds") == "0"
