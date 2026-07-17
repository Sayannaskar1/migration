from dataclasses import dataclass, field
from abc import ABC, abstractmethod


@dataclass
class MigrateResult:
    table: str
    storage_type: str
    strategy: str
    rows: int = 0
    duration_ms: int = 0
    success: bool = False
    error: str | None = None


class DataMover(ABC):
    @abstractmethod
    def migrate(self, table_info, sf_creds: dict, db_creds: dict, storage_creds: dict | None = None) -> MigrateResult:
        ...
