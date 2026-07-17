from abc import ABC, abstractmethod
from typing import Any


class SourceConnector(ABC):
    @abstractmethod
    def test_connection(self) -> str:
        ...

    @abstractmethod
    def extract_project(self, project_dir: str = "", databases=None, on_progress=None) -> dict:
        ...

    @abstractmethod
    def execute_sql(self, sql: str) -> dict:
        ...

    @abstractmethod
    def close(self):
        ...


class TargetConnector(ABC):
    @abstractmethod
    def test_connection(self) -> str:
        ...

    @abstractmethod
    def deploy(self, sql_statements: list[dict], dry_run: bool = False, on_error: str = "stop") -> list[dict]:
        ...

    @abstractmethod
    def execute_sql(self, sql: str) -> dict:
        ...

    @abstractmethod
    def close(self):
        ...


class Translator(ABC):
    @abstractmethod
    def transpile(self, sql: str, source_type: str = "snowflake", target_type: str = "databricks", target_version: str = "") -> str | None:
        ...
