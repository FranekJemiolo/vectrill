"""
Query Planner Python DSL

This module provides a Python DSL for building lazy expression graphs
that can be compiled into logical plans and executed by the Vectrill engine.
"""

from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
import json


@dataclass
class Node:
    """Represents a node in the lazy expression graph."""
    op: str
    inputs: List['Node']
    attrs: Dict[str, Any]
    
    def __post_init__(self):
        # Convert inputs to Node objects if they're Stream objects
        for i, input_node in enumerate(self.inputs):
            if isinstance(input_node, Stream):
                self.inputs[i] = input_node.node
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary for serialization."""
        return {
            'op': self.op,
            'inputs': [node.to_dict() for node in self.inputs],
            'attrs': self.attrs
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Node':
        """Create node from dictionary."""
        inputs = [cls.from_dict(input_data) for input_data in data.get('inputs', [])]
        return cls(
            op=data['op'],
            inputs=inputs,
            attrs=data.get('attrs', {})
        )


class Stream:
    """User-facing API for building query streams."""
    
    def __init__(self, node: Node):
        self.node = node
    
    def filter(self, expr: str) -> 'Stream':
        """Filter records based on a predicate expression."""
        filter_node = Node("filter", [self.node], {"expr": expr})
        return Stream(filter_node)
    
    def map(self, expr: str) -> 'Stream':
        """Map/transform records using an expression."""
        map_node = Node("map", [self.node], {"expr": expr})
        return Stream(map_node)
    
    def group_by(self, key: Union[str, List[str]]) -> 'Stream':
        """Group records by key(s)."""
        if isinstance(key, str):
            key = [key]
        group_by_node = Node("group_by", [self.node], {"key": key})
        return Stream(group_by_node)
    
    def window(self, spec: str) -> 'Stream':
        """Apply window specification."""
        window_node = Node("window", [self.node], {"spec": spec})
        return Stream(window_node)
    
    def agg(self, spec: Dict[str, str]) -> 'Stream':
        """Apply aggregation specification."""
        agg_node = Node("agg", [self.node], {"spec": spec})
        return Stream(agg_node)
    
    def project(self, columns: Union[str, List[str]]) -> 'Stream':
        """Project/select columns."""
        if isinstance(columns, str):
            columns = [columns]
        project_node = Node("project", [self.node], {"columns": columns})
        return Stream(project_node)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stream to dictionary for serialization."""
        return self.node.to_dict()
    
    def to_json(self) -> str:
        """Convert stream to JSON string."""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Stream':
        """Create stream from dictionary."""
        node = Node.from_dict(data)
        return cls(node)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Stream':
        """Create stream from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


def source(name: str) -> Stream:
    """Create a source stream."""
    source_node = Node("source", [], {"name": name})
    return Stream(source_node)


# Convenience functions for common patterns
def kafka_source(topic: str, bootstrap_servers: str = "localhost:9092") -> Stream:
    """Create a Kafka source stream."""
    attrs = {
        "name": "kafka",
        "topic": topic,
        "bootstrap_servers": bootstrap_servers
    }
    source_node = Node("source", [], attrs)
    return Stream(source_node)


def file_source(path: str, format: str = "csv") -> Stream:
    """Create a file source stream."""
    attrs = {
        "name": "file",
        "path": path,
        "format": format
    }
    source_node = Node("source", [], attrs)
    return Stream(source_node)


def memory_source(data: List[Dict[str, Any]]) -> Stream:
    """Create a memory source stream from data."""
    attrs = {
        "name": "memory",
        "data": data
    }
    source_node = Node("source", [], attrs)
    return Stream(source_node)


# Window specification helpers
def tumbling_window(duration: str) -> str:
    """Create a tumbling window specification."""
    return f"tumbling({duration})"


def sliding_window(duration: str, slide: str) -> str:
    """Create a sliding window specification."""
    return f"sliding({duration}, {slide})"


def session_window(timeout: str) -> str:
    """Create a session window specification."""
    return f"session({timeout})"


# Aggregation helpers
def sum_agg(column: str) -> Dict[str, str]:
    """Create sum aggregation."""
    return {column: "sum"}


def avg_agg(column: str) -> Dict[str, str]:
    """Create average aggregation."""
    return {column: "avg"}


def count_agg(column: str = "*") -> Dict[str, str]:
    """Create count aggregation."""
    return {column: "count"}


def min_agg(column: str) -> Dict[str, str]:
    """Create min aggregation."""
    return {column: "min"}


def max_agg(column: str) -> Dict[str, str]:
    """Create max aggregation."""
    return {column: "max"}


def multi_agg(specs: Dict[str, str]) -> Dict[str, str]:
    """Create multiple aggregations."""
    return specs


# Example usage and testing
if __name__ == "__main__":
    # Example pipeline from the documentation
    stream = (
        kafka_source("sensor_data")
        .filter("temp > 20")
        .map("temp_f = temp * 1.8 + 32")
        .group_by("device_id")
        .window(tumbling_window("10s"))
        .agg(avg_agg("temp_f"))
    )
    
    print("Generated DSL:")
    print(stream.to_json())
    
    # Another example
    stream2 = (
        file_source("data.csv", "csv")
        .filter("price > 100")
        .project(["symbol", "price", "volume"])
        .group_by("symbol")
        .window(sliding_window("1m", "30s"))
        .agg({
            "price": "avg",
            "volume": "sum",
            "count": "count"
        })
    )
    
    print("\nSecond example:")
    print(stream2.to_json())
