#!/usr/bin/env python3
"""
Snowflake to Databricks Migration Agent

Converts a Snowflake SQL project (tables, views, procedures, functions, schemas)
to Databricks SQL with dependency ordering, validation, and documentation.

Usage:
    python main.py /path/to/snowflake/project [--output /path/to/output]
"""

import argparse
import sys
from pathlib import Path

from orchestrator import MigrationOrchestrator


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Snowflake to Databricks Migration Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py ./snowflake_project
    python main.py ./snowflake_project --output ./databricks_output
    python main.py ./snowflake_project --skip-validation
        """,
    )
    parser.add_argument(
        "project_path",
        type=str,
        help="Path to the Snowflake SQL project directory",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="output",
        help="Output directory for converted SQL and reports (default: output)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip the validation step",
    )
    parser.add_argument(
        "--skip-docs",
        action="store_true",
        help="Skip documentation generation",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    project_path = Path(args.project_path)
    if not project_path.exists():
        print(f"Error: Project path does not exist: {project_path}")
        return 1
    if not project_path.is_dir():
        print(f"Error: Project path is not a directory: {project_path}")
        return 1

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    orchestrator = MigrationOrchestrator(
        project_path=str(project_path),
        output_dir=str(output_dir),
    )

    try:
        artifacts = orchestrator.run()
        print(f"\nOutput artifacts:")
        for name, path in artifacts.items():
            print(f"  {name}: {path}")
        return 0
    except Exception as e:
        print(f"\nError during migration: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
