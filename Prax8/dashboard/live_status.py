"""Live status dashboard for Maria's Reliability Lab.

Runs inside airflow-scheduler (so it can reach postgres + trino over the
internal docker network and read alerts/ via the bind mount). The facilitator
projects this on a second screen during the session; rows flip color when
chaos hits.

Data sources:
  - Row counts:    Trino `iceberg_datalake.prd.<table>` (read-only fast path)
  - Snapshot version + last-merge time: snapshot_ledger in Postgres
  - Recent alerts: alerts/slack.log (tail)
  - Status:        alerts/pager.log entries within the last 60s

Refresh every REFRESH_SEC seconds. Errors during a tick are absorbed so a
flaky network / dropped Trino connection produces stale data, not a crash.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import trino
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text


REFRESH_SEC = float(os.environ.get("DASHBOARD_REFRESH_SEC", "2"))
TABLES = ["prd.users", "prd.orders", "prd.events"]
ALERTS_DIR = Path(os.environ.get("ALERT_DIR", "/opt/airflow/alerts"))
PAGER_FRESHNESS_SEC = 60


def _pg_dsn() -> str:
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    db = os.environ.get("POSTGRES_DB", "airflow")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    return f"postgresql://{user}:{password}@{host}/{db}"


def _trino_conn():
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="dashboard",
        catalog="iceberg_datalake",
    )


_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(s: str) -> str:
    return _ANSI.sub("", s)


def trino_row_count(table: str) -> int | None:
    """COUNT(*) for `prd.<name>` via Trino. Returns None when the query fails
    (e.g. the table hasn't been merged yet)."""
    try:
        conn = _trino_conn()
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM iceberg_datalake.{table}")
        ((n,),) = cur.fetchall()
        return int(n)
    except Exception:
        return None


def ledger_stats(table: str) -> tuple[int, datetime | None]:
    """Return (number_of_merges_so_far, latest_recorded_at). Versions are
    counted by ledger rows so we get a friendly v1/v2/v3 instead of the
    raw bigint snapshot_id."""
    try:
        with psycopg2.connect(_pg_dsn()) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), MAX(recorded_at) FROM snapshot_ledger "
                "WHERE table_name = %s",
                (table,),
            )
            count, last = cur.fetchone()
            return int(count or 0), last
    except Exception:
        return 0, None


def table_status(table: str) -> str:
    """HEALTHY unless the table appears in pager.log within the last minute."""
    pager = ALERTS_DIR / "pager.log"
    if not pager.exists():
        return "HEALTHY"
    cutoff = datetime.now() - timedelta(seconds=PAGER_FRESHNESS_SEC)
    try:
        for raw_line in reversed(pager.read_text().splitlines()):
            line = _strip_ansi(raw_line)
            m = re.search(r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", line)
            if not m:
                continue
            ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            if ts < cutoff:
                break
            if table in line:
                return "QUARANTINED"
    except Exception:
        pass
    return "HEALTHY"


def recent_alerts(limit: int = 5) -> list[str]:
    slack = ALERTS_DIR / "slack.log"
    if not slack.exists():
        return []
    lines = slack.read_text().splitlines()
    return [_strip_ansi(line).strip() for line in lines[-limit:]]


def render() -> Panel:
    now = datetime.now().strftime("%H:%M:%S")

    table = Table(show_header=True, header_style="bold cyan", expand=True, pad_edge=False)
    table.add_column("Table", style="bold", no_wrap=True)
    table.add_column("Snapshot", justify="right")
    table.add_column("Rows", justify="right")
    table.add_column("Last Merge", justify="right")
    table.add_column("Status", justify="right")

    for name in TABLES:
        rows = trino_row_count(name)
        version, last_at = ledger_stats(name)
        status = table_status(name)

        rows_text = f"{rows:,}" if rows is not None else "—"
        version_text = f"v{version}" if version else "—"
        last_text = last_at.strftime("%H:%M:%S") if last_at else "—"

        if status == "QUARANTINED":
            status_text = Text("❌ QUARANTINED", style="bold red")
            row_style = "red"
        else:
            status_text = Text("✅ HEALTHY", style="green")
            row_style = ""

        table.add_row(
            name, version_text, rows_text, last_text, status_text, style=row_style
        )

    alerts = recent_alerts()
    if alerts:
        alert_block = Text("\n".join(alerts))
    else:
        alert_block = Text("  [no alerts]", style="dim")

    body = Table.grid(expand=True)
    body.add_row(table)
    body.add_row(Text(""))
    body.add_row(Text("Recent alerts (last 5):", style="bold"))
    body.add_row(alert_block)

    return Panel(
        body,
        title=f"[bold]Maria's Replication Lab[/bold]",
        subtitle=f"[dim]{now}[/dim]",
        border_style="cyan",
    )


def main() -> int:
    import time

    console = Console()
    try:
        with Live(render(), console=console, refresh_per_second=4, screen=False) as live:
            while True:
                time.sleep(REFRESH_SEC)
                try:
                    live.update(render())
                except Exception as e:  # noqa: BLE001
                    # Survive a single bad tick; show the error in-frame.
                    live.update(Panel(Text(f"render error: {e}", style="red")))
    except KeyboardInterrupt:
        console.print("\n[dim]Dashboard stopped.[/dim]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
