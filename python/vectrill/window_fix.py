# Test the window functionality separately
import sys
sys.path.insert(0, 'python')
import vectrill

# Create a working window implementation
class WorkingWindowSpec:
    def __init__(self, partition_by=None, order_by=None):
        self.partition_by = partition_by or []
        self.order_by = order_by or []
    
    def partition_by(self, *columns):
        self.partition_by = list(columns)
        return self
    
    def order_by(self, *columns):
        self.order_by = list(columns)
        return self

class WorkingWindow:
    @staticmethod
    def partition_by(*columns):
        return WorkingWindowSpec(partition_by=list(columns))
    
    @staticmethod
    def order_by(*columns):
        return WorkingWindowSpec(order_by=list(columns))

# Test the working version
window_spec = WorkingWindow.partition_by('group').order_by('id')
print('Working window spec:', window_spec)
print('partition_by:', window_spec.partition_by)
print('order_by:', window_spec.order_by)
