from __future__ import annotations

import json
import os
import sys

from shared.xray_profile_registry import build_xray_config


def main() -> int:
    config, _metadata = build_xray_config(dict(os.environ))
    json.dump(config, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
