import json
from pathlib import Path

CHECKPOINT_FILE = Path("state/checkpoint.json")


def load_checkpoint() -> str | None:
    if not CHECKPOINT_FILE.exists():
        return None

    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
        return data.get("after")


def save_checkpoint(after: str | None) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"after": after}, f, indent=4)
