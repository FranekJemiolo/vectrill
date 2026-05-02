"""Window function module for Vectrill compatibility with pandas tests"""

from typing import List, Optional, Union, Dict, Any


class WindowTransform:
    """Window transformation specification"""
    
    def __init__(self, partition_by: Optional[Union[str, List[str]]] = None, 
                 order_by: Optional[Union[str, List[str]]] = None):
        if isinstance(partition_by, str):
            self.partition_columns = [partition_by]
        else:
            self.partition_columns = partition_by or []
            
        if isinstance(order_by, str):
            self.order_columns = [order_by]
        else:
            self.order_columns = order_by or []
    
    def partition_by(self, *columns) -> 'WindowTransform':
        """Set partition by columns"""
        new_columns = []
        for col in columns:
            if isinstance(col, str):
                new_columns.append(col)
            elif hasattr(col, 'name'):
                new_columns.append(col.name)
            else:
                new_columns.append(str(col))
        
        new_transform = WindowTransform()
        new_transform.partition_columns = list(self.partition_columns) + new_columns
        new_transform.order_columns = list(self.order_columns)
        return new_transform
    
    def order_by(self, *columns) -> 'WindowTransform':
        """Set order by columns"""
        new_columns = []
        for col in columns:
            if isinstance(col, str):
                new_columns.append(col)
            elif hasattr(col, 'name'):
                new_columns.append(col.name)
            else:
                new_columns.append(str(col))
        
        new_transform = WindowTransform()
        new_transform.partition_columns = list(self.partition_columns)
        new_transform.order_columns = list(self.order_columns) + new_columns
        return new_transform
    
    def to_rust_spec(self) -> Dict[str, Any]:
        """Convert to Rust specification"""
        return {
            'partition_by': self.partition_columns,
            'order_by': self.order_columns
        }


def partition_by(*columns) -> WindowTransform:
    """Create a window transform with partition by columns"""
    return WindowTransform(partition_by=list(columns))


def order_by(*columns) -> WindowTransform:
    """Create a window transform with order by columns"""
    return WindowTransform(order_by=list(columns))


# Create a default window instance for common usage
window = WindowTransform()
