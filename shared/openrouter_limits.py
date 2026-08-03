"""Разбор rate-limit заголовков OpenRouter.

Вынесено в shared, потому что один и тот же заголовок читают три места
(worker vision, worker text, admin health-probe), и расхождение реализаций уже
стоило инцидента: admin парсил значение сырым ``float()``, тогда как OpenRouter
отдаёт ``X-RateLimit-Reset`` в МИЛЛИСЕКУНДАХ.

2026-08-01 17:20Z проба здоровья упёрлась в дневной лимит бесплатного тарифа,
получила 429 с ``X-RateLimit-Reset: 1785628800000`` и записала
``in_quarantine_until = 1785628800000.0`` — то есть 29 апреля 58554 года.
14 из 16 бесплатных моделей ушли в вечный карантин, а алерт
FrontierOpenRouterModelQuarantineBurst «самоизлечивался» лишь по TTL ключей
в Redis и возвращался на следующие сутки.
"""

from __future__ import annotations

import time

# Верхняя граница правдоподобного epoch в секундах (~2286 год).
# Всё, что больше, — практически наверняка миллисекунды.
_MAX_PLAUSIBLE_EPOCH_SEC = 10_000_000_000

# Ниже этого значение трактуем как относительное смещение («через N секунд»),
# а не как абсолютный epoch.
_MIN_PLAUSIBLE_EPOCH_SEC = 1_000_000_000

# Ни один ответ провайдера не должен уводить модель из ротации больше чем на
# сутки. Потолок делает худший случай самовосстанавливающимся, даже если формат
# заголовка снова изменится.
MAX_QUARANTINE_SEC = 86_400.0


def parse_rate_limit_reset(value: str | None, *, now: float | None = None) -> float | None:
    """Привести ``X-RateLimit-Reset`` к абсолютному epoch-времени в секундах.

    Поддерживает три формы, которые встречаются на практике: миллисекунды,
    секунды и относительное смещение в секундах. Возвращает ``None``, если
    заголовок пуст или неразбираем.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        numeric = float(raw)
    except (TypeError, ValueError):
        return None

    current = time.time() if now is None else now
    if numeric > _MAX_PLAUSIBLE_EPOCH_SEC:
        return numeric / 1000.0
    if numeric > _MIN_PLAUSIBLE_EPOCH_SEC:
        return numeric
    if numeric > current + 60:
        return numeric
    return current + max(0.0, numeric)


def clamp_quarantine_until(
    candidate: float | None,
    *,
    now: float | None = None,
    max_seconds: float = MAX_QUARANTINE_SEC,
) -> float:
    """Ограничить срок карантина сверху.

    Даже корректный парсер не защищает от мусора на стороне провайдера, а
    бесконечный карантин выводит модель из ротации навсегда и молча. Возвращает
    ``0.0``, если карантин уже истёк или не задан.
    """
    current = time.time() if now is None else now
    if not candidate or candidate <= current:
        return 0.0
    return min(float(candidate), current + max_seconds)
