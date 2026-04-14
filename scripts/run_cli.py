"""Unified CLI entrypoint.

Example:
    PYTHONPATH=src python scripts/run_cli.py run-all --days 7 --top-n 20
"""

from ytradar.cli import main


if __name__ == "__main__":
    main()
