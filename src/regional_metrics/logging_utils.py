from __future__ import annotations

import logging
from pathlib import Path


def configure_logging(config: dict[str, object], log_file: Path | None = None) -> None:
    kwargs = {
        "level": getattr(logging, str(config.get("level", "INFO")).upper(), logging.INFO),
        "format": str(config.get("format", "%(asctime)s - %(levelname)s - %(message)s")),
        "datefmt": str(config.get("datefmt", "%Y-%m-%d %H:%M:%S")),
    }
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        kwargs["filename"] = str(log_file)
        kwargs["filemode"] = "w"

    logging.basicConfig(**kwargs)
