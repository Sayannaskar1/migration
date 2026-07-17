from agents.data_migration.data_migration_manager import DataMigrationManager
from agents.data_migration.mover_base import MigrateResult

__all__ = ["DataMigrationEngine", "MigrateResult"]


class DataMigrationEngine:
    def __init__(self):
        self._manager = DataMigrationManager()

    def migrate(self, storage_report, sf_creds: dict, db_creds: dict, s3_creds: dict | None = None, cloud_provider: str | None = None) -> list[MigrateResult]:
        return self._manager.migrate(storage_report, sf_creds, db_creds, s3_creds, cloud_provider)
