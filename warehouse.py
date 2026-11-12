"""Load landing files into a local DuckDB star-schema warehouse."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SETUP_SQL = PROJECT_ROOT / "sql" / "01_setup.sql"

DIMENSIONS = (
    ("CURATED.DIM_ROCKET", "rocket_key", "SEQ_ROCKET_KEY", "rocket_name"),
    ("CURATED.DIM_LAUNCH_SITE", "launch_site_key", "SEQ_LAUNCH_SITE_KEY", "site_name"),
    ("CURATED.DIM_AGENCY", "agency_key", "SEQ_AGENCY_KEY", "agency_name"),
    ("CURATED.DIM_MISSION_TYPE", "mission_type_key", "SEQ_MISSION_TYPE_KEY", "mission_type_name"),
    ("CURATED.DIM_ORBIT", "orbit_key", "SEQ_ORBIT_KEY", "orbit_name"),
)

LAUNCH_FIELDS = (
    "source_name",
    "source_id",
    "company",
    "location",
    "launch_datetime",
    "rocket_name",
    "mission_name",
    "rocket_status",
    "mission_status",
    "mission_type",
    "orbit",
    "agency",
    "payload_mass_kg",
)


def _hash(*values: Any) -> str:
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def initialize(connection: Any) -> None:
    """Create warehouse schemas, sequences, raw tables, and unknown dimension rows."""
    connection.execute(SETUP_SQL.read_text(encoding="utf-8"))


def _insert_raw_files(connection: Any, run_dir: Path, run_id: str) -> int:
    launch_count = 0
    api_dir = run_dir / "api"
    if api_dir.exists():
        for path in sorted(api_dir.glob("*.jsonl")):
            with path.open("r", encoding="utf-8") as handle:
                for payload_index, line in enumerate(handle, start=1):
                    record = json.loads(line)
                    fetched_at = datetime.fromisoformat(record["fetched_at"])
                    connection.execute(
                        """
                        INSERT INTO RAW.API_PAYLOADS
                          (run_id, source_name, payload_index, fetched_at, payload_json, source_file)
                        VALUES (?, ?, ?, ?, ?::JSON, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        [
                            run_id,
                            record["source_name"],
                            payload_index,
                            fetched_at,
                            json.dumps(record["payload"], ensure_ascii=False),
                            path.name,
                        ],
                    )

    csv_path = run_dir / "csv" / "launches.csv"
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                values = [row.get(field) or None for field in LAUNCH_FIELDS]
                connection.execute(
                    """
                    INSERT INTO RAW.LAUNCHES
                      (run_id, source_name, source_id, company, location, launch_datetime,
                       rocket_name, mission_name, rocket_status, mission_status, mission_type,
                       orbit, agency, payload_mass_kg, source_file)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                    """,
                    [run_id, *values, csv_path.name],
                )
                launch_count += 1
    return launch_count


def _dimension_key(
    connection: Any,
    *,
    table: str,
    key_column: str,
    sequence: str,
    natural_column: str,
    natural_value: str,
    attributes: tuple[Any, ...],
    extra_columns: tuple[str, ...] = (),
    extra_values: tuple[Any, ...] = (),
) -> int:
    if natural_value == "Unknown":
        return 0
    attribute_hash = _hash(*attributes)
    current = connection.execute(
        f"SELECT {key_column}, attribute_hash FROM {table} "
        f"WHERE {natural_column} = ? AND is_current",
        [natural_value],
    ).fetchone()
    if current and current[1] == attribute_hash:
        return int(current[0])
    now = datetime.now(timezone.utc)
    if current:
        connection.execute(
            f"UPDATE {table} SET effective_to = ?, is_current = FALSE WHERE {key_column} = ?",
            [now, current[0]],
        )

    surrogate_key = int(connection.execute(f"SELECT nextval('CURATED.{sequence}')").fetchone()[0])
    insert_columns = [key_column, natural_column, *extra_columns, "attribute_hash", "effective_from", "is_current"]
    insert_values = [surrogate_key, natural_value]
    insert_values.extend(extra_values)
    insert_values.extend([attribute_hash, now, True])
    placeholders = ", ".join("?" for _ in insert_values)
    connection.execute(
        f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({placeholders})",
        insert_values,
    )
    return surrogate_key


def _rocket_attributes(name: str) -> tuple[str, str | None]:
    upper_name = name.upper()
    families = ("FALCON", "ATLAS", "DELTA", "SOYUZ", "LONG MARCH", "ARIANE", "VEGA", "PROTON")
    family = next((item.title() for item in families if upper_name.startswith(item)), name.split()[0])
    parents = (
        ("FALCON HEAVY", "Falcon 9"),
        ("FALCON 9", "Falcon 1"),
        ("ATLAS V", "Atlas II"),
        ("DELTA IV", "Delta III"),
    )
    parent = next((value for prefix, value in parents if upper_name.startswith(prefix)), None)
    return family, parent


def _date_key(connection: Any, launch_date: date | None) -> int:
    if launch_date is None:
        return 0
    current = connection.execute(
        "SELECT date_key FROM CURATED.DIM_DATE WHERE full_date = ?", [launch_date]
    ).fetchone()
    if current:
        return int(current[0])
    date_key = int(connection.execute("SELECT nextval('CURATED.SEQ_DATE_KEY')").fetchone()[0])
    connection.execute(
        """
        INSERT INTO CURATED.DIM_DATE
          (date_key, full_date, calendar_year, calendar_quarter, month_number, month_name,
           day_of_month, day_of_week, is_weekend)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            date_key,
            launch_date,
            launch_date.year,
            (launch_date.month - 1) // 3 + 1,
            launch_date.month,
            launch_date.strftime("%B"),
            launch_date.day,
            launch_date.isoweekday(),
            launch_date.isoweekday() in (6, 7),
        ],
    )
    return date_key


def _load_dimensions(connection: Any, rows: list[tuple[Any, ...]]) -> None:
    for row in rows:
        (
            _run_id,
            source_name,
            source_id,
            launch_name,
            launch_timestamp,
            rocket_name,
            site_name,
            agency_name,
            mission_type_name,
            orbit_name,
            payload_mass,
            _launch_status,
            _success_flag,
            launch_date,
            _source_loaded_at,
        ) = row
        family_name, parent_name = _rocket_attributes(rocket_name)
        _dimension_key(
            connection,
            table="CURATED.DIM_ROCKET",
            key_column="rocket_key",
            sequence="SEQ_ROCKET_KEY",
            natural_column="rocket_name",
            natural_value=rocket_name,
            attributes=(family_name, parent_name),
            extra_columns=("family_name", "parent_rocket_name", "parent_rocket_key"),
            extra_values=(family_name, parent_name, None),
        )
        _dimension_key(
            connection,
            table="CURATED.DIM_LAUNCH_SITE",
            key_column="launch_site_key",
            sequence="SEQ_LAUNCH_SITE_KEY",
            natural_column="site_name",
            natural_value=site_name,
            attributes=(site_name,),
        )
        _dimension_key(
            connection,
            table="CURATED.DIM_AGENCY",
            key_column="agency_key",
            sequence="SEQ_AGENCY_KEY",
            natural_column="agency_name",
            natural_value=agency_name,
            attributes=(agency_name,),
        )
        _dimension_key(
            connection,
            table="CURATED.DIM_MISSION_TYPE",
            key_column="mission_type_key",
            sequence="SEQ_MISSION_TYPE_KEY",
            natural_column="mission_type_name",
            natural_value=mission_type_name,
            attributes=(mission_type_name,),
        )
        _dimension_key(
            connection,
            table="CURATED.DIM_ORBIT",
            key_column="orbit_key",
            sequence="SEQ_ORBIT_KEY",
            natural_column="orbit_name",
            natural_value=orbit_name,
            attributes=(orbit_name,),
        )
        if launch_timestamp:
            _date_key(connection, launch_timestamp.date())

    connection.execute(
        """
        UPDATE CURATED.DIM_ROCKET AS child
        SET parent_rocket_key = parent.rocket_key
        FROM CURATED.DIM_ROCKET AS parent
        WHERE child.is_current AND parent.is_current
          AND child.parent_rocket_name = parent.rocket_name
          AND child.parent_rocket_key IS DISTINCT FROM parent.rocket_key
        """
    )


def _load_facts(connection: Any, rows: list[tuple[Any, ...]]) -> None:
    for row in rows:
        (
            _run_id,
            source_name,
            source_id,
            launch_name,
            launch_timestamp,
            rocket_name,
            site_name,
            agency_name,
            mission_type_name,
            orbit_name,
            payload_mass,
            launch_status,
            success_flag,
            launch_date,
            source_loaded_at,
        ) = row
        dimension_values = []
        for table, key_column, _sequence, natural_column in DIMENSIONS:
            natural_value = {
                "rocket_name": rocket_name,
                "site_name": site_name,
                "agency_name": agency_name,
                "mission_type_name": mission_type_name,
                "orbit_name": orbit_name,
            }[natural_column]
            key = connection.execute(
                f"SELECT {key_column} FROM {table} WHERE {natural_column} = ? AND is_current",
                [natural_value],
            ).fetchone()
            dimension_values.append(int(key[0]) if key else 0)
        date_key = _date_key(connection, launch_date)
        content_hash = _hash(
            source_name, source_id, launch_name, launch_timestamp, rocket_name, site_name,
            agency_name, mission_type_name, orbit_name, payload_mass, launch_status, success_flag,
        )
        connection.execute(
            """
            INSERT INTO CURATED.FACT_LAUNCH
              (launch_key, source_name, source_id, launch_name, launch_timestamp, rocket_key,
               launch_site_key, agency_key, mission_type_key, orbit_key, date_key,
               payload_mass_kg, launch_status, success_flag, source_hash, source_loaded_at)
            VALUES
              (nextval('CURATED.SEQ_LAUNCH_KEY'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (source_name, source_id) DO UPDATE SET
              launch_name = excluded.launch_name,
              launch_timestamp = excluded.launch_timestamp,
              rocket_key = excluded.rocket_key,
              launch_site_key = excluded.launch_site_key,
              agency_key = excluded.agency_key,
              mission_type_key = excluded.mission_type_key,
              orbit_key = excluded.orbit_key,
              date_key = excluded.date_key,
              payload_mass_kg = excluded.payload_mass_kg,
              launch_status = excluded.launch_status,
              success_flag = excluded.success_flag,
              source_hash = excluded.source_hash,
              source_loaded_at = excluded.source_loaded_at,
              last_updated_at = now()
            WHERE source_hash <> excluded.source_hash
              OR rocket_key <> excluded.rocket_key
              OR launch_site_key <> excluded.launch_site_key
              OR agency_key <> excluded.agency_key
              OR mission_type_key <> excluded.mission_type_key
              OR orbit_key <> excluded.orbit_key
              OR date_key <> excluded.date_key
            """,
            [
                source_name,
                source_id,
                launch_name,
                launch_timestamp,
                *dimension_values,
                date_key,
                payload_mass,
                launch_status,
                success_flag,
                content_hash,
                source_loaded_at,
            ],
        )


def load_run(connection: Any, run_dir: Path, run_id: str) -> None:
    """Load one landing batch, maintain SCD2 dimensions, merge facts, and audit."""
    initialize(connection)
    audit_key = int(connection.execute("SELECT nextval('OPS.SEQ_AUDIT_KEY')").fetchone()[0])
    started_at = datetime.now(timezone.utc)
    connection.execute(
        """
        INSERT INTO OPS.ETL_AUDIT
          (audit_key, run_id, pipeline_name, source_object, status, source_rows, started_at)
        VALUES (?, ?, 'SPACE_MISSIONS_LOAD', 'LANDING_FILES', 'RUNNING', 0, ?)
        """,
        [audit_key, run_id, started_at],
    )
    connection.commit()
    try:
        connection.begin()
        _insert_raw_files(connection, run_dir, run_id)
        rows = connection.execute(
            """
            SELECT run_id, source_name, source_id, launch_name, launch_timestamp,
                   rocket_name, site_name, agency_name, mission_type_name, orbit_name,
                   payload_mass_kg, launch_status, success_flag, launch_date, source_loaded_at
            FROM CURATED.VW_LAUNCH_CLEAN
            ORDER BY source_name, source_id
            """
        ).fetchall()
        _load_dimensions(connection, rows)
        _load_facts(connection, rows)
        fact_count = int(connection.execute("SELECT COUNT(*) FROM CURATED.FACT_LAUNCH").fetchone()[0])
        connection.execute(
            """
            UPDATE OPS.ETL_AUDIT
            SET status = 'SUCCEEDED',
                source_rows = ?,
                target_rows = ?,
                completed_at = current_timestamp
            WHERE audit_key = ?
            """,
            [len(rows), fact_count, audit_key],
        )
        connection.commit()
    except Exception as exc:
        connection.rollback()
        connection.execute(
            """
            UPDATE OPS.ETL_AUDIT
            SET status = 'FAILED', completed_at = current_timestamp, error_message = ?
            WHERE audit_key = ?
            """,
            [str(exc), audit_key],
        )
        connection.commit()
        raise
