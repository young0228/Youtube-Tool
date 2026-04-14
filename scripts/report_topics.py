"""Run topic reporting CLI.

Example:
    PYTHONPATH=src python scripts/report_topics.py report-topics --top-n 15 --md-path data/exports/topic_report.md
"""

from ytradar.cli import main


if __name__ == "__main__":
    main()
