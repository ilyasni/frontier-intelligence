import argparse
import asyncio
import json
import sys

sys.path.insert(0, "/app")

from worker.services.semantic_clustering import run_semantic_clustering


def main() -> None:
    parser = argparse.ArgumentParser(description="Run semantic dedupe and stable trend clustering.")
    parser.add_argument("--workspace", dest="workspace_id", default=None)
    args = parser.parse_args()
    result = asyncio.run(run_semantic_clustering(args.workspace_id))
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
