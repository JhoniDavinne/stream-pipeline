"""Gera eventos JSONL para testes locais ou futura entrada Kafka."""

import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

CATEGORIES = ("eletronicos", "livros", "casa")


def generate_event(event_id: int) -> dict:
    """Cria um evento deterministico, com uma pequena taxa de dados invalidos."""
    category = random.choice(CATEGORIES + ("categoria-invalida",))
    amount = round(random.uniform(-20, 200), 2)
    return {
        "event_id": event_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "category": category,
        "amount": amount,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=20)
    parser.add_argument("--interval", type=float, default=0.0)
    parser.add_argument("--output", default="data/input/events.jsonl")
    args = parser.parse_args()

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as file:
        for event_id in range(args.events):
            file.write(json.dumps(generate_event(event_id), ensure_ascii=False) + "\n")
            file.flush()
            if args.interval:
                time.sleep(args.interval)

    print(f"{args.events} eventos escritos em {output}")


if __name__ == "__main__":
    main()
