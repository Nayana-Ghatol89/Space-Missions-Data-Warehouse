import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from python.warehouse import _dimension_key, initialize, load_run


FIELDS = [
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
]


class WarehouseTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.connection = duckdb.connect(str(self.root / "test.duckdb"))
        initialize(self.connection)

    def tearDown(self):
        self.connection.close()
        self.temp_dir.cleanup()

    def create_run(self, run_id, rocket_name):
        run_dir = self.root / run_id
        csv_dir = run_dir / "csv"
        csv_dir.mkdir(parents=True)
        row = {
            "source_name": "kaggle",
            "source_id": "mission-1",
            "company": "Example Agency",
            "location": "Example Spaceport",
            "launch_datetime": "2024-01-08 12:30:00",
            "rocket_name": rocket_name,
            "mission_name": "Example Payload",
            "rocket_status": "",
            "mission_status": "Success",
            "mission_type": "Science",
            "orbit": "LEO",
            "agency": "Example Agency",
            "payload_mass_kg": "1200",
        }
        with (csv_dir / "launches.csv").open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerow(row)
        return run_dir

    def test_repeat_load_is_idempotent(self):
        run_dir = self.create_run("run-1", "Example-1")
        load_run(self.connection, run_dir, "run-1")
        load_run(self.connection, run_dir, "run-1")
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM CURATED.FACT_LAUNCH").fetchone()[0],
            1,
        )
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM RAW.LAUNCHES").fetchone()[0],
            1,
        )

    def test_new_source_values_update_fact(self):
        first = self.create_run("run-1", "Falcon 9")
        second = self.create_run("run-2", "Falcon Heavy")
        load_run(self.connection, first, "run-1")
        load_run(self.connection, second, "run-2")
        versions = self.connection.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE is_current)
            FROM CURATED.DIM_ROCKET
            WHERE rocket_name IN ('Falcon 9', 'Falcon Heavy')
            """
        ).fetchone()
        self.assertEqual(versions, (2, 2))
        fact = self.connection.execute(
            """
            SELECT rocket.rocket_name
            FROM CURATED.FACT_LAUNCH fact
            JOIN CURATED.DIM_ROCKET rocket USING (rocket_key)
            WHERE fact.source_id = 'mission-1'
            """
        ).fetchone()
        self.assertEqual(fact[0], "Falcon Heavy")

    def test_dimension_attribute_change_creates_scd2_version(self):
        run_dir = self.create_run("run-1", "Falcon 9")
        load_run(self.connection, run_dir, "run-1")
        old_key = self.connection.execute(
            "SELECT rocket_key FROM CURATED.DIM_ROCKET WHERE rocket_name = 'Falcon 9' AND is_current"
        ).fetchone()[0]
        new_key = _dimension_key(
            self.connection,
            table="CURATED.DIM_ROCKET",
            key_column="rocket_key",
            sequence="SEQ_ROCKET_KEY",
            natural_column="rocket_name",
            natural_value="Falcon 9",
            attributes=("Falcon Family Updated", None),
            extra_columns=("family_name", "parent_rocket_name", "parent_rocket_key"),
            extra_values=("Falcon Family Updated", None, None),
        )
        versions = self.connection.execute(
            """
            SELECT COUNT(*), COUNT(*) FILTER (WHERE is_current)
            FROM CURATED.DIM_ROCKET WHERE rocket_name = 'Falcon 9'
            """
        ).fetchone()
        self.assertNotEqual(old_key, new_key)
        self.assertEqual(versions, (2, 1))


if __name__ == "__main__":
    unittest.main()
