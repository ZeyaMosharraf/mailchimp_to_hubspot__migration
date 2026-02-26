import json
from pathlib import Path

CHECKPOINT_FILE = Path("state/checkpoint.json")


def load_checkpoint() -> int | None:
    if not CHECKPOINT_FILE.exists():
        return None

    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
        return data.get("offset")


def save_checkpoint(offset: int | None) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"offset": offset}, f, indent=4)
