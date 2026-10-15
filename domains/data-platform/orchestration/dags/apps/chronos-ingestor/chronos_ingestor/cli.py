from __future__ import annotations

import argparse
from .ingest import ingest_chronos


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Busca dados do Chronos e salva em banco SQL")
    parser.add_argument("--date", default=None, help="Busca um unico dia. Ex: 2026-05-01")
    parser.add_argument("--from", dest="date_start", default=None, help="Data inicial. Ex: 2026-05-01")
    parser.add_argument("--to", dest="date_end", default=None, help="Data final. Ex: 2026-05-07")
    parser.add_argument("--modules", default="", help="IDs de modulos separados por virgula. Ex: 13,14")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.date:
        args.date_start = args.date
        args.date_end = args.date
    ingest_chronos(args.date_start, args.date_end, args.modules)
