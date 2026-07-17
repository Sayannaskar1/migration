from collections import defaultdict
from typing import Optional
from agents.project_loader import ProjectInventory, ParsedObject
from parser.sql_parser import ObjectType


class DependencyGraph:
    def __init__(self):
        self.edges: dict[str, list[str]] = defaultdict(list)
        self.reverse_edges: dict[str, list[str]] = defaultdict(list)
        self.nodes: dict[str, ParsedObject] = {}
        self.cycles: list[str] = []

    def add_object(self, obj: ParsedObject):
        name = obj.name.lower()
        self.nodes[name] = obj

    def add_dependency(self, obj_name: str, depends_on: str):
        obj_key = obj_name.lower()
        dep_key = depends_on.lower()
        if obj_key != dep_key:
            if dep_key not in self.edges[obj_key]:
                self.edges[obj_key].append(dep_key)
            if obj_key not in self.reverse_edges[dep_key]:
                self.reverse_edges[dep_key].append(obj_key)

    def has_cycles(self) -> bool:
        visited: set[str] = set()
        rec_stack: set[str] = set()
        self.cycles = []

        def dfs(node: str):
            visited.add(node)
            rec_stack.add(node)
            for neighbor in self.edges.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    self.cycles.append(node)
                    return True
            rec_stack.remove(node)
            return False

        for node in list(self.nodes.keys()):
            if node not in visited:
                dfs(node)

        return len(self.cycles) > 0

    def topological_sort(self) -> list[ParsedObject]:
        if self.has_cycles():
            print(
                f"Warning: Circular dependencies detected: {self.cycles}. "
                "Sorting will be approximate."
            )

        in_degree: dict[str, int] = defaultdict(int)
        for node in self.nodes:
            in_degree[node] = 0
        for deps in self.edges.values():
            for dep in deps:
                if dep in self.nodes:
                    in_degree[dep] += 1

        queue: list[str] = [
            n for n, d in in_degree.items() if d == 0
        ]
        result: list[ParsedObject] = []
        while queue:
            node = queue.pop(0)
            if node in self.nodes:
                result.append(self.nodes[node])
            for neighbor in self.reverse_edges.get(node, []):
                if neighbor in in_degree:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)

        for node in self.nodes:
            if node not in [o.name.lower() for o in result]:
                result.append(self.nodes[node])

        return result

    def object_type_order(self) -> dict:
        return {
            ObjectType.SCHEMA: 0,
            ObjectType.TABLE: 1,
            ObjectType.VIEW: 2,
            ObjectType.FUNCTION: 3,
            ObjectType.PROCEDURE: 4,
            ObjectType.UNKNOWN: 5,
        }

    def get_deployment_order(self, with_type_sort: bool = True) -> list[ParsedObject]:
        topo = self.topological_sort()

        if with_type_sort:
            type_order = self.object_type_order()
            type_buckets: dict[int, list[ParsedObject]] = defaultdict(list)
            for obj in topo:
                order = type_order.get(obj.object_type, 99)
                type_buckets[order].append(obj)

            sorted_result: list[ParsedObject] = []
            for key in sorted(type_buckets.keys()):
                sorted_result.extend(type_buckets[key])
            return sorted_result

        return topo


def analyze_dependencies(inventory: ProjectInventory) -> DependencyGraph:
    graph = DependencyGraph()
    all_names_lower = {o.name.lower() for o in inventory.all_objects}

    for obj in inventory.all_objects:
        graph.add_object(obj)
        for dep_raw in obj.dependencies:
            dep_parts = dep_raw.strip().strip('"').split(".")
            dep_name = dep_parts[-1].strip().strip('"')
            dep_lower = dep_name.lower()
            if dep_lower in all_names_lower:
                graph.add_dependency(obj.name, dep_name)
            for cte_name in obj.cte_names:
                cte_lower = cte_name.lower()
                if cte_lower == dep_lower:
                    break

    return graph
