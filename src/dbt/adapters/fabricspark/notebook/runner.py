"""dbt command execution with logging and artifact persistence.

Handles the full lifecycle: deps install, elementary bootstrap,
command execution, structured log persistence, and artifact archival.
All logs and artifacts are written to the lakehouse Files section.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import yaml


LAKEHOUSE_LOG_ROOT = "/lakehouse/default/Files/logs/dbt"


@dataclass
class DbtResult:
    """Result of a dbt command execution."""

    success: bool
    exit_code: int
    duration_ms: int
    stdout: str
    stderr: str
    invocation_id: str | None
    log_dir: str


def run_dbt(
    command: str,
    project_dir: str,
    *,
    log_root: str = LAKEHOUSE_LOG_ROOT,
) -> DbtResult:
    """Execute a dbt command with full observability.

    Steps:
      1. Install dbt packages (deps)
      2. Bootstrap elementary tables if configured and missing
      3. Execute the dbt command
      4. Persist logs and artifacts to lakehouse

    Args:
        command: Full dbt command string, e.g. "dbt run --select tag:orders".
            The command is parsed into arguments. Must start with "dbt".
        project_dir: Absolute path to the dbt project directory.
        log_root: Lakehouse log root directory.

    Returns:
        DbtResult with execution details.

    Raises:
        RuntimeError: If the dbt command fails (after persisting logs).
    """
    start_time = time.time()

    # Parse the command string into a base command and extra args
    parts = shlex.split(command)
    if not parts or parts[0] != "dbt":
        raise ValueError(f"Command must start with 'dbt', got: {command!r}")
    dbt_subcommand = parts[1] if len(parts) > 1 else "run"
    extra_args = parts[2:]

    job_name = os.environ.get("DBT_JOB_NAME", "unknown")

    # Set up lakehouse log directory
    run_ts_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    log_dir = f"{log_root}/{run_ts_path}"
    os.makedirs(log_dir, exist_ok=True)

    # Install dbt packages
    _run_dbt_deps(project_dir)

    # Bootstrap elementary if needed
    _bootstrap_elementary(project_dir)

    # Build the dbt command args
    dbt_args = [
        "dbt",
        dbt_subcommand,
        "--project-dir",
        project_dir,
        "--profiles-dir",
        project_dir,
        "--log-format",
        "json",
        "--log-path",
        log_dir,
        *extra_args,
    ]

    proc = subprocess.run(
        dbt_args, capture_output=True, text=True, cwd=project_dir
    )
    print(proc.stdout)
    if proc.stderr:
        print(proc.stderr)

    duration_ms = int((time.time() - start_time) * 1000)
    print(f"Duration: {duration_ms}ms")
    print(f"ExitCode: {proc.returncode}")

    invocation_id = None
    try:
        if proc.returncode != 0:
            _raise_with_error_context(dbt_subcommand, proc)
    finally:
        invocation_id = _persist_artifacts(
            project_dir, log_dir, job_name, dbt_subcommand, proc, duration_ms
        )

    return DbtResult(
        success=proc.returncode == 0,
        exit_code=proc.returncode,
        duration_ms=duration_ms,
        stdout=proc.stdout or "",
        stderr=proc.stderr or "",
        invocation_id=invocation_id,
        log_dir=log_dir,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _run_dbt_deps(project_dir: str) -> None:
    """Install dbt packages (elementary, dbt_utils, etc.)."""
    deps_proc = subprocess.run(
        [
            "dbt",
            "deps",
            "--project-dir",
            project_dir,
            "--profiles-dir",
            project_dir,
        ],
        capture_output=True,
        text=True,
        cwd=project_dir,
    )
    print(deps_proc.stdout)
    if deps_proc.returncode != 0:
        print(f"Warning: dbt deps failed: {deps_proc.stderr}")


def _bootstrap_elementary(project_dir: str) -> None:
    """Bootstrap elementary monitoring tables if schema is empty.

    Reads the elementary schema name from dbt_project.yml, checks if tables
    exist in that schema, and materializes them if missing.
    """
    try:
        dbt_project_path = os.path.join(project_dir, "dbt_project.yml")
        if not os.path.exists(dbt_project_path):
            print("No dbt_project.yml found — skipping elementary bootstrap")
            return

        with open(dbt_project_path) as f:
            dbt_cfg = yaml.safe_load(f)

        elementary_schema = (
            dbt_cfg.get("models", {}).get("elementary", {}).get("+schema")
        )
        if not elementary_schema:
            print(
                "No elementary schema found in dbt_project.yml — skipping bootstrap"
            )
            return

        # Validate schema name to prevent injection
        if not re.match(r"^[a-zA-Z0-9_]+$", elementary_schema):
            raise ValueError(
                f"Invalid elementary schema name: {elementary_schema}"
            )

        check_proc = subprocess.run(
            [
                "dbt",
                "show",
                "--inline",
                f"select count(*) as cnt from INFORMATION_SCHEMA.TABLES "
                f"where TABLE_SCHEMA = '{elementary_schema}'",
                "--project-dir",
                project_dir,
                "--profiles-dir",
                project_dir,
            ],
            capture_output=True,
            text=True,
            cwd=project_dir,
        )

        has_tables = (
            check_proc.returncode == 0 and "| 0" not in check_proc.stdout
        )
        if not has_tables:
            print(
                f"Elementary schema '{elementary_schema}' is empty "
                f"— bootstrapping with dbt run -s elementary"
            )
            elem_proc = subprocess.run(
                [
                    "dbt",
                    "run",
                    "-s",
                    "elementary",
                    "--project-dir",
                    project_dir,
                    "--profiles-dir",
                    project_dir,
                ],
                capture_output=True,
                text=True,
                cwd=project_dir,
            )
            print(elem_proc.stdout)
            if elem_proc.returncode != 0:
                print(
                    f"Warning: elementary bootstrap failed: {elem_proc.stderr}"
                )
            else:
                print("Elementary tables bootstrapped successfully")
        else:
            print(
                f"Elementary schema '{elementary_schema}' already has tables "
                f"— skipping bootstrap"
            )

    except Exception as e:
        print(f"Warning: elementary bootstrap check failed: {e}")


def _raise_with_error_context(
    subcommand: str, proc: subprocess.CompletedProcess[str]
) -> None:
    """Extract error context from dbt output and raise RuntimeError."""
    full_output = proc.stderr or proc.stdout or "No output captured"

    # Extract lines after first occurrence of "error" (case-insensitive)
    error_section = full_output
    lines = full_output.split("\n")
    for i, line in enumerate(lines):
        if "error" in line.lower():
            error_section = "\n".join(lines[i:])
            break

    raise RuntimeError(
        f"dbt {subcommand} failed with exit code {proc.returncode}"
        f"\n\n{error_section}"
    )


def _persist_artifacts(
    project_dir: str,
    log_dir: str,
    job_name: str,
    subcommand: str,
    proc: subprocess.CompletedProcess[str],
    duration_ms: int,
) -> str | None:
    """Persist dbt logs and artifacts into an invocation-specific folder.

    Structure: logs/dbt/{YYYY}/{MM}/{DD}/{invocation_id}/
      - dbt.log
      - run_results.json
      - manifest.json

    Returns the invocation_id if available.
    """
    invocation_id = None
    results_src = os.path.join(project_dir, "target", "run_results.json")

    # Extract invocation_id from run_results.json
    try:
        if os.path.exists(results_src):
            with open(results_src) as f:
                run_results = json.load(f)
            invocation_id = run_results.get("metadata", {}).get(
                "invocation_id"
            )
    except Exception as e:
        print(
            f"Warning: could not read invocation_id from run_results.json: {e}"
        )

    # Fallback folder name when invocation_id is unavailable
    run_id = datetime.now(timezone.utc).strftime("%H%M%S")
    folder_name = invocation_id or f"{job_name}_{run_id}"
    inv_dir = f"{log_dir}/{folder_name}"
    os.makedirs(inv_dir, exist_ok=True)

    # Move dbt.log into invocation folder
    _persist_log_file(
        log_dir, inv_dir, subcommand, proc, duration_ms
    )

    # Copy run_results.json and manifest.json
    _persist_dbt_artifacts(project_dir, inv_dir, results_src)

    print(f"Invocation folder: {inv_dir}")
    return invocation_id


def _persist_log_file(
    log_dir: str,
    inv_dir: str,
    subcommand: str,
    proc: subprocess.CompletedProcess[str],
    duration_ms: int,
) -> None:
    """Move or create dbt.log in the invocation folder."""
    try:
        dbt_log_src = os.path.join(log_dir, "dbt.log")
        log_dest = f"{inv_dir}/dbt.log"

        if os.path.exists(dbt_log_src):
            os.rename(dbt_log_src, log_dest)
            print(f"dbt JSON logs persisted to lakehouse: {log_dest}")
        else:
            # Fallback: write captured stdout/stderr
            with open(log_dest, "w") as f:
                f.write(
                    f"# dbt {subcommand} | duration={duration_ms}ms "
                    f"| exit_code={proc.returncode}\n"
                )
                f.write(
                    f"# timestamp="
                    f"{datetime.now(timezone.utc).isoformat()}\n\n"
                )
                f.write(proc.stdout or "")
                if proc.stderr:
                    f.write("\n--- STDERR ---\n")
                    f.write(proc.stderr)
            print(f"Captured output saved to lakehouse: {log_dest}")
    except Exception as log_err:
        print(f"Warning: failed to persist logs to lakehouse: {log_err}")


def _persist_dbt_artifacts(
    project_dir: str, inv_dir: str, results_src: str
) -> None:
    """Copy run_results.json and manifest.json into invocation folder."""
    try:
        if os.path.exists(results_src):
            shutil.copy2(results_src, f"{inv_dir}/run_results.json")
            print(
                f"run_results.json persisted to lakehouse: "
                f"{inv_dir}/run_results.json"
            )

        manifest_src = os.path.join(project_dir, "target", "manifest.json")
        if os.path.exists(manifest_src):
            shutil.copy2(manifest_src, f"{inv_dir}/manifest.json")
            print(
                f"manifest.json persisted to lakehouse: "
                f"{inv_dir}/manifest.json"
            )
    except Exception as artifact_err:
        print(f"Warning: failed to persist dbt artifacts: {artifact_err}")
