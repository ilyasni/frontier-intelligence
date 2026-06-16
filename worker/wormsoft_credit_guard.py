"""Wormsoft rolling credit-window throttle guard.

Wormsoft exposes no account-level remaining-credit endpoint, so usage is estimated
from our own rolling counter in :class:`ProviderBudgetManager`. This guard sheds
Wormsoft as a routing candidate once estimated usage in the configured window
crosses the soft cap (and hard cap), diverting load to fallback providers.
"""
from __future__ import annotations

import logging
from typing import Any

from shared.llm_control_plane import EXECUTION_ROLE_SHADOW, ExecutionReceipt
from shared.llm_routing import PROVIDER_WORMSOFT, normalize_provider

logger = logging.getLogger(__name__)

# Fallback for the Wormsoft rolling credit window when the setting is missing
# (mirrors shared.config WORMSOFT_CREDIT_WINDOW_SECONDS default). The real value
# comes from settings/.env; this guards only mocked/partial settings objects.
WORMSOFT_CREDIT_WINDOW_SECONDS_DEFAULT = 18000

REASON_OK = "ok"
REASON_SOFT_CAP = "wormsoft_credit_soft_cap"
REASON_HARD_CAP = "wormsoft_credit_hard_cap"


class WormsoftCreditGuard:
    """Soft/hard cap throttle over the Wormsoft rolling credit window."""

    def __init__(self, budget_manager: Any, *, settings: Any) -> None:
        self._budget_manager = budget_manager
        self._settings = settings

    def _window_seconds(self) -> int:
        return max(
            60,
            int(
                getattr(self._settings, "wormsoft_credit_window_seconds", WORMSOFT_CREDIT_WINDOW_SECONDS_DEFAULT)
                or WORMSOFT_CREDIT_WINDOW_SECONDS_DEFAULT
            ),
        )

    def _enabled(self) -> bool:
        return bool(getattr(self._settings, "wormsoft_credit_throttle_enabled", False))

    async def allow(self, *, provider: str, execution_role: str) -> tuple[bool, str]:
        """Return whether Wormsoft may be used given current rolling credit usage."""
        if normalize_provider(provider) != PROVIDER_WORMSOFT:
            return True, REASON_OK
        if not self._enabled():
            return True, REASON_OK
        limit = float(getattr(self._settings, "wormsoft_credit_window_limit", 500000.0) or 0.0)
        if limit <= 0:
            return True, REASON_OK
        soft_ratio = float(getattr(self._settings, "wormsoft_credit_soft_cap_ratio", 0.8) or 0.8)
        hard_ratio = float(getattr(self._settings, "wormsoft_credit_hard_cap_ratio", 0.98) or 0.98)
        if execution_role == EXECUTION_ROLE_SHADOW:
            soft_ratio = float(
                getattr(self._settings, "wormsoft_credit_soft_cap_shadow_ratio", soft_ratio) or soft_ratio
            )
        soft_cap = max(0.0, min(limit, limit * max(0.0, soft_ratio)))
        hard_cap = max(0.0, min(limit, limit * max(0.0, hard_ratio)))
        try:
            used = await self._budget_manager.credit_window_usage(
                provider=PROVIDER_WORMSOFT,
                window_seconds=self._window_seconds(),
            )
        except Exception:
            logger.debug("wormsoft_credit_window_read_failed", exc_info=True)
            return True, REASON_OK
        if hard_cap > 0 and used >= hard_cap:
            return False, REASON_HARD_CAP
        if soft_cap > 0 and used >= soft_cap:
            return False, REASON_SOFT_CAP
        return True, REASON_OK

    async def record(self, receipt: ExecutionReceipt) -> None:
        """Record actual Wormsoft credit spend into the rolling window."""
        if normalize_provider(receipt.actual_provider) != PROVIDER_WORMSOFT:
            return
        if not self._enabled():
            return
        credits = float(receipt.actual_cost or 0.0)
        if credits <= 0.0:
            return
        try:
            await self._budget_manager.add_credit_usage(
                provider=PROVIDER_WORMSOFT,
                credits=credits,
                window_seconds=self._window_seconds(),
            )
        except Exception:
            logger.debug("wormsoft_credit_window_record_failed", exc_info=True)
