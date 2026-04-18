import sys
import os
from unittest.mock import MagicMock

# Allow imports from project root without installing packages
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from tests.stub_policy import GLOBAL_MAGICMOCK_STUB_MODULES

# ── pydantic_settings stub ────────────────────────────────────────────────────
# BaseSettings must be a real class so that `class Settings(BaseSettings)` works.
class _BaseSettings:
    model_config = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

_ps_stub = MagicMock()
_ps_stub.BaseSettings = _BaseSettings
_ps_stub.SettingsConfigDict = dict
sys.modules.setdefault("pydantic_settings", _ps_stub)

# ── apscheduler stub ──────────────────────────────────────────────────────────
_aps_stub = MagicMock()
_aps_stub.schedulers = MagicMock()
_aps_stub.schedulers.asyncio = MagicMock()
_aps_stub.schedulers.asyncio.AsyncIOScheduler = MagicMock
_aps_stub.triggers = MagicMock()
_aps_stub.triggers.cron = MagicMock()
_aps_stub.triggers.cron.CronTrigger = MagicMock()
sys.modules.setdefault("apscheduler", _aps_stub)
sys.modules.setdefault("apscheduler.schedulers", _aps_stub.schedulers)
sys.modules.setdefault("apscheduler.schedulers.asyncio", _aps_stub.schedulers.asyncio)
sys.modules.setdefault("apscheduler.triggers", _aps_stub.triggers)
sys.modules.setdefault("apscheduler.triggers.cron", _aps_stub.triggers.cron)

# ── telethon stubs with real exception/type classes ──────────────────────────
class _FloodWaitError(Exception):
    def __init__(self, *a, **kw):
        self.seconds = kw.get("seconds", 0)
        super().__init__()

class _SessionRevokedError(Exception):
    pass

class _UserDeactivatedBanError(Exception):
    pass

class _TelegramMessage:
    """Minimal stand-in for telethon.tl.types.Message used in isinstance checks."""

class _MessageMediaPhoto:
    pass

class _MessageMediaDocument:
    pass

_telethon_errors = MagicMock()
_telethon_errors.FloodWaitError = _FloodWaitError
_telethon_errors.SessionRevokedError = _SessionRevokedError
_telethon_errors.UserDeactivatedBanError = _UserDeactivatedBanError

_telethon_types = MagicMock()
_telethon_types.Message = _TelegramMessage
_telethon_types.MessageMediaPhoto = _MessageMediaPhoto
_telethon_types.MessageMediaDocument = _MessageMediaDocument

_telethon = MagicMock()
_telethon.TelegramClient = MagicMock
_telethon.errors = _telethon_errors

for _name, _val in [
    ("telethon", _telethon),
    ("telethon.errors", _telethon_errors),
    ("telethon.tl", MagicMock()),
    ("telethon.tl.types", _telethon_types),
    ("telethon.network", MagicMock()),
    ("telethon.network.connection", MagicMock()),
    ("telethon.network.connection.tcpmtproxy", MagicMock()),
]:
    sys.modules.setdefault(_name, _val)

# ── simple MagicMock stubs (список — tests/stub_policy.py, тест test_global_mock_policy) ──
for _mod in GLOBAL_MAGICMOCK_STUB_MODULES:
    sys.modules.setdefault(_mod, MagicMock())
