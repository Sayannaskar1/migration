import time
from .mover_base import MigrateResult
from .external_data_mover import ExternalDataMover
from .internal_data_mover import InternalDataMover
from .iceberg_data_mover import IcebergDataMover


class DataMigrationManager:
    def __init__(self):
        self.movers = {
            "external": ExternalDataMover(),
            "internal": InternalDataMover(),
            "iceberg": IcebergDataMover(),
        }

    def migrate(self, storage_report, sf_creds: dict, db_creds: dict, storage_creds: dict | None = None, cloud_provider: str | None = None) -> list[MigrateResult]:
        results = []

        for table in storage_report.external_tables:
            mover = self.movers["external"]
            result = mover.migrate(table, sf_creds, db_creds, storage_creds)
            results.append(result)

        for table in storage_report.iceberg_tables:
            mover = self.movers["iceberg"]
            result = mover.migrate(table, sf_creds, db_creds, storage_creds)
            results.append(result)

        for table in storage_report.internal_tables:
            mover = self.movers["internal"]
            result = mover.migrate(table, sf_creds, db_creds, storage_creds, cloud_override=cloud_provider)
            results.append(result)

        return results

    def _select_internal_mover(self, cloud_provider: str | None) -> InternalDataMover:
        return self.movers["internal"]
