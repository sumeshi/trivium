"""A dependency-free client for Trivium's protocol-v1 JSON-lines executable."""

import argparse
import itertools
import json
import subprocess


class Trivium:
    def __init__(self, executable, workspace):
        self.process = subprocess.Popen(
            [executable, "--workspace", workspace],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )
        self.ids = itertools.count(1)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.process.stdin.close()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
        finally:
            self.process.stdout.close()

    def call(self, method, params=None):
        request_id = next(self.ids)
        request = {"version": 1, "id": request_id, "method": method, "params": params or {}}
        self.process.stdin.write(json.dumps(request, ensure_ascii=False) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("Trivium closed the protocol stream; see stderr")
        response = json.loads(line)
        if response.get("version") != 1 or response.get("id") != request_id:
            raise RuntimeError("Unexpected Trivium protocol response")
        if not response.get("ok"):
            error = response["error"]
            raise RuntimeError(f"{error['code']}: {error['message']}")
        return response["result"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", default="trivium-cli")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--source", help="Explicitly import this CSV/JSON/JSONL source")
    parser.add_argument("--project", help="Saved case UUID; defaults to the newest case")
    parser.add_argument("--search", default="")
    args = parser.parse_args()
    with Trivium(args.cli, args.workspace) as engine:
        project = args.project
        if args.source:
            project = engine.call("import", {"path": args.source})["meta"]["id"]
        if not project:
            projects = engine.call("list")
            if not projects:
                print("No saved cases. Use --source to import one.")
                return
            project = projects[0]["meta"]["id"]
        result = engine.call("query", {"projectId": project, "search": args.search, "limit": 50})
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
