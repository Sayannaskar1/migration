from io import StringIO
from pathlib import Path
from typing import Optional
from parser.sql_parser import parse_sql_file, parse_sql_content, ParsedObject, ObjectType


class ProjectInventory:
    def __init__(self, project_path: Path):
        self.project_path = project_path
        self.by_type: dict[str, list[ParsedObject]] = {}
        self.all_objects: list[ParsedObject] = []

    def _unique_by_type(self, objs: list) -> int:
        seen = set()
        for o in objs:
            seen.add(o.name.lower())
        return len(seen)

    @staticmethod
    def _pluralize(key: str) -> str:
        if key.endswith("s"):
            return key
        if key.endswith("y") and len(key) > 2 and key[-2] not in "aeiou":
            return key[:-1] + "ies"
        return key + "s"

    def summary(self) -> dict:
        unique = set()
        for o in self.all_objects:
            unique.add(o.name.lower())
        result = {
            "project": str(self.project_path),
            "total_objects": len(self.all_objects),
            "unique_objects": len(unique),
            "unknown": len(self.by_type.get(ObjectType.UNKNOWN, [])),
        }
        for type_name, items in self.by_type.items():
            if type_name == ObjectType.UNKNOWN:
                continue
            plural = self._pluralize(type_name)
            if type_name in (ObjectType.TABLE, ObjectType.VIEW, ObjectType.PROCEDURE, ObjectType.FUNCTION):
                result[plural] = self._unique_by_type(items)
            else:
                result[plural] = len(items)
        return result

    def get_by_name(self, name: str) -> Optional[ParsedObject]:
        for obj in self.all_objects:
            if obj.name.lower() == name.lower():
                return obj
        return None


def _is_sql_file(path: Path) -> bool:
    return path.suffix.lower() in (".sql", ".ddl")


def _categorize(obj: ParsedObject, inventory: ProjectInventory):
    key = obj.object_type if obj.object_type != ObjectType.UNKNOWN else ObjectType.UNKNOWN
    inventory.by_type.setdefault(key, []).append(obj)
    inventory.all_objects.append(obj)


def load_project(project_path: str) -> ProjectInventory:
    path = Path(project_path)
    if not path.exists():
        raise FileNotFoundError(f"Project path does not exist: {project_path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Project path is not a directory: {project_path}")

    inventory = ProjectInventory(path)
    sql_files = sorted(path.rglob("*.sql")) + sorted(path.rglob("*.ddl"))
    seen_files = set()
    for file_path in sql_files:
        if file_path in seen_files:
            continue
        seen_files.add(file_path)
        try:
            if "(" in file_path.name:
                new_name = file_path.name.replace("(", "")
                new_path = file_path.parent / new_name
                if not new_path.exists():
                    file_path.rename(new_path)
                    file_path = new_path
            objects = parse_sql_file(file_path)
            for obj in objects:
                obj.name = obj.name.rstrip("(")
                _categorize(obj, inventory)
        except Exception as e:
            print(f"Warning: Failed to parse {file_path}: {e}")

    return inventory


def load_project_from_tree(project_name: str, tree: dict[str, str]) -> ProjectInventory:
    """Load project from an in-memory tree dict {relative_path: content}."""
    inventory = ProjectInventory(Path(project_name))

    sql_files = sorted(k for k in tree if k.endswith(".sql") or k.endswith(".ddl"))
    seen_files = set()
    for rel_path in sql_files:
        if rel_path in seen_files:
            continue
        seen_files.add(rel_path)
        try:
            content = tree[rel_path]
            if "(" in Path(rel_path).name:
                continue
            objects = parse_sql_content(content, file_path=Path(rel_path))
            for obj in objects:
                obj.name = obj.name.rstrip("(")
                _categorize(obj, inventory)
        except Exception as e:
            print(f"Warning: Failed to parse {rel_path}: {e}")

    return inventory
