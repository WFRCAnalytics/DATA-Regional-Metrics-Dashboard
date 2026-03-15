from __future__ import annotations

import argparse
from pathlib import Path

from regional_metrics.io_spatial import read_spatial
from regional_metrics.validation import compare_frames


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two local spatial outputs.")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()

    summary = compare_frames(read_spatial(args.baseline), read_spatial(args.candidate))
    print(summary)


if __name__ == "__main__":
    main()
