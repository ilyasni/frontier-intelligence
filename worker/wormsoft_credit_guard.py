"""Wormsoft rolling credit-window throttle guard.

Wormsoft exposes no account-level remaining-credit endpoint, so usage is estimated
from our own rolling counter in :class:`ProviderBudgetManager`. This guard sheds
Wormsoft as a routing candidate once estimated usage in the configured window
crosses the soft cap (and hard cap), diverting load to fallback providers. Successful
usage is still metered when enforcement is disabled so rollout can run in shadow mode.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from shared.llm_control_plane import EXECUTION_ROLE_SHADOW, ExecutionReceipt
from shared.llm_routing import PROVIDER_WORMSOFT, normalize_provider
from shared.metrics import (
    note_wormsoft_credit_accounting_error,
    note_wormsoft_credit_estimate,
    set_wormsoft_credit_window,
)
from worker.wormsoft_credit_estimator import WormsoftCreditEstimator

logger = logging.getLogger(__name__)

# Fallback for the Wormsoft rolling credit window when the setting is missing
# (mirrors shared.config WORMSOFT_CREDIT_WINDOW_SECONDS default). The real value
# comes from settings/.env; this guards only mocked/partial settings objects.
WORMSOFT_CREDIT_WINDOW_SECONDS_DEFAULT = 14400

REASON_OK = "ok"
REASON_SOFT_CAP = "wormsoft_credit_soft_cap"
REASON_HARD_CAP = "wormsoft_credit_hard_cap"
REASON_READ_FAILED = "wormsoft_credit_read_failed"
REASON_PRICING_UNKNOWN = "wormsoft_credit_pricing_unknown"

# Env flag to fail *closed* (shed Wormsoft) when the rolling-window read errors,
# instead of the default fail-open. Read via getattr on settings first (so a future
# shared.config field wins), then WORMSOFT_CREDIT_FAIL_CLOSED env, default False =
# current fail-open behaviour. Applied to the primary role only.
_ENV_FAIL_CLOSED = "WORMSOFT_CREDIT_FAIL_CLOSED"


class WormsoftCreditGuard:
    """Soft/hard cap throttle over the Wormsoft rolling credit window."""

    def __init__(
        self,
        budget_manager: Any,
        *,
        settings: Any,
        service_name: str = "worker",
        estimator: WormsoftCreditEstimator | None = None,
    ) -> None:
        self._budget_manager = budget_manager
        self._settings = settings
        self._service_name = service_name
        self._estimator = estimator or WormsoftCreditEstimator()

    def _window_seconds(self) -> int:
        return max(
            60,
            int(
                getattr(
                    self._settings,
                    "wormsoft_credit_window_seconds",
                    WORMSOFT_CREDIT_WINDOW_SECONDS_DEFAULT,
                )
                or WORMSOFT_CREDIT_WINDOW_SECONDS_DEFAULT
            ),
        )

    def _enabled(self) -> bool:
        return bool(getattr(self._settings, "wormsoft_credit_throttle_enabled", False))

    def _fail_closed(self) -> bool:
        """Whether a rolling-window read error should shed Wormsoft (fail-closed).

        Prefers a settings attribute if present (e.g. a future shared.config field),
        then the WORMSOFT_CREDIT_FAIL_CLOSED env var. Defaults to False so the current
        fail-open behaviour is preserved unless explicitly hardened.
        """
        setting = getattr(self._settings, "wormsoft_credit_fail_closed", None)
        if setting is not None:
            return bool(setting)
        raw = str(os.environ.get(_ENV_FAIL_CLOSED, "") or "").strip().lower()
        return raw in {"1", "true", "yes", "on"}

    async def allow(
        self,
        *,
        provider: str,
        execution_role: str,
        requested_model: str = "",
    ) -> tuple[bool, str]:
        """Return whether Wormsoft may be used given current rolling credit usage."""
        if normalize_provider(provider) != PROVIDER_WORMSOFT:
            return True, REASON_OK
        if not self._enabled():
            return True, REASON_OK
        limit = float(getattr(self._settings, "wormsoft_credit_window_limit", 3000000.0) or 0.0)
        if limit <= 0:
            return True, REASON_OK
        if requested_model and not self._estimator.supports_model(requested_model):
            note_wormsoft_credit_accounting_error(
                self._service_name,
                "estimate",
                "unknown_model",
            )
            logger.error(
                "wormsoft_credit_pricing_unknown requested_model=%s",
                requested_model,
            )
            return False, REASON_PRICING_UNKNOWN
        soft_ratio = float(getattr(self._settings, "wormsoft_credit_soft_cap_ratio", 0.8) or 0.8)
        hard_ratio = float(getattr(self._settings, "wormsoft_credit_hard_cap_ratio", 0.95) or 0.95)
        shadow_soft_ratio = soft_ratio
        if execution_role == EXECUTION_ROLE_SHADOW:
            shadow_soft_ratio = float(
                getattr(
                    self._settings,
                    "wormsoft_credit_soft_cap_shadow_ratio",
                    soft_ratio,
                )
                or soft_ratio
            )
        shadow_soft_cap = max(0.0, min(limit, limit * max(0.0, shadow_soft_ratio)))
        hard_cap = max(0.0, min(limit, limit * max(0.0, hard_ratio)))
        try:
            used = await self._budget_manager.credit_window_usage(
                provider=PROVIDER_WORMSOFT,
                window_seconds=self._window_seconds(),
            )
        except Exception:
            note_wormsoft_credit_accounting_error(
                self._service_name,
                "read",
                "redis_error",
            )
            # Optional shadow work must never spend blind. Primary remains fail-open by
            # default and can be hardened explicitly after Redis availability is proven.
            if execution_role == EXECUTION_ROLE_SHADOW or self._fail_closed():
                logger.warning("wormsoft_credit_window_read_failed_fail_closed", exc_info=True)
                return False, REASON_READ_FAILED
            logger.debug("wormsoft_credit_window_read_failed", exc_info=True)
            return True, REASON_OK
        set_wormsoft_credit_window(
            self._service_name,
            used=used,
            limit=limit,
            soft_cap_ratio=soft_ratio,
            hard_cap_ratio=hard_ratio,
        )
        if hard_cap > 0 and used >= hard_cap:
            return False, REASON_HARD_CAP
        if (
            execution_role == EXECUTION_ROLE_SHADOW
            and shadow_soft_cap > 0
            and used >= shadow_soft_cap
        ):
            return False, REASON_SOFT_CAP
        return True, REASON_OK

    async def record(self, receipt: ExecutionReceipt) -> None:
        """Record actual Wormsoft credit spend into the rolling window."""
        if normalize_provider(receipt.actual_provider) != PROVIDER_WORMSOFT:
            return
        budget = dict(receipt.budget_attribution or {})
        pricing_model = str(budget.get("model") or receipt.requested_model or "").strip()
        estimate = self._estimator.try_estimate(
            requested_model=pricing_model,
            prompt_tokens=int(budget.get("prompt_tokens") or 0),
            completion_tokens=int(budget.get("completion_tokens") or 0),
            cached_prompt_tokens=int(budget.get("cached_prompt_tokens") or 0),
        )
        if estimate is None:
            note_wormsoft_credit_accounting_error(
                self._service_name,
                "estimate",
                "unknown_model",
            )
            logger.error(
                "wormsoft_credit_record_skipped_unknown_model requested_model=%s actual_model=%s",
                pricing_model or "<empty>",
                receipt.actual_model or "<empty>",
            )
            return
        credits = estimate.total_credits
        note_wormsoft_credit_estimate(
            self._service_name,
            receipt.task,
            estimate.requested_model,
            receipt.actual_model,
            receipt.execution_role,
            input_credits=estimate.input_credits,
            output_credits=estimate.output_credits,
            cache_credits=estimate.cache_credits,
            total_credits=credits,
        )
        if credits <= 0.0:
            return
        try:
            used = await self._budget_manager.add_credit_usage(
                provider=PROVIDER_WORMSOFT,
                credits=credits,
                window_seconds=self._window_seconds(),
            )
            set_wormsoft_credit_window(
                self._service_name,
                used=used,
                limit=float(
                    getattr(self._settings, "wormsoft_credit_window_limit", 3000000.0) or 0.0
                ),
                soft_cap_ratio=float(
                    getattr(self._settings, "wormsoft_credit_soft_cap_ratio", 0.8) or 0.8
                ),
                hard_cap_ratio=float(
                    getattr(self._settings, "wormsoft_credit_hard_cap_ratio", 0.95) or 0.95
                ),
            )
        except Exception:
            note_wormsoft_credit_accounting_error(
                self._service_name,
                "write",
                "redis_error",
            )
            logger.warning("wormsoft_credit_window_record_failed", exc_info=True)
