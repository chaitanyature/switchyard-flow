import argparse
import json
import sys
import time
from pathlib import Path

from .engine import Engine
from .worker import HANDLERS, execute


def main():
    parser = argparse.ArgumentParser(description="Durable DAG dispatcher")
    parser.add_argument("database")
    commands = parser.add_subparsers(dest="command", required=True)
    submit = commands.add_parser("submit")
    submit.add_argument("run")
    submit.add_argument("spec", type=Path)
    pool = commands.add_parser("pool")
    pool.add_argument("name")
    pool.add_argument("size", type=int)
    work = commands.add_parser("work")
    work.add_argument("owner")
    work.add_argument(
        "--until-idle",
        action="store_true",
        help="exit when no job is immediately claimable",
    )
    commands.add_parser("status").add_argument("run")
    events = commands.add_parser("events")
    events.add_argument("run")
    events.add_argument("--after", type=int, default=0)
    args = parser.parse_args()
    engine = Engine(args.database)
    if args.command == "submit":
        result = {"created": engine.submit(args.run, json.loads(args.spec.read_text()))}
    elif args.command == "pool":
        engine.set_capacity(args.name, args.size)
        return
    elif args.command == "work":
        while True:
            lease = engine.claim(args.owner)
            if lease is None:
                if args.until_idle:
                    return
                time.sleep(0.25)
            else:
                execute(engine, lease, HANDLERS)
    elif args.command == "events":
        result = engine.events(args.run, args.after)
    else:
        result = engine.report(args.run)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError) as exc:
        sys.exit(str(exc))
