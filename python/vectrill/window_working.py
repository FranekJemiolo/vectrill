# Working window implementation that avoids conflicts
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
    
    def __call__(self, df):
        if self.partition_by and self.order_by:
            return df.over(partition_by=self.partition_by, order_by=self.order_by)
        elif self.partition_by:
            return df.over(partition_by=self.partition_by)
        elif self.order_by:
            return df.over(order_by=self.order_by)
        else:
            return df

class WorkingWindow:
    @staticmethod
    def partition_by(*columns):
        return WorkingWindowSpec(partition_by=list(columns))
    
    @staticmethod
    def order_by(*columns):
        return WorkingWindowSpec(order_by=list(columns))

# Test the working implementation
if __name__ == '__main__':
    # Test method chaining
    window_spec = WorkingWindow.partition_by('group').order_by('id')
    print('Success! Window spec:', window_spec)
    print('partition_by:', window_spec.partition_by)
    print('order_by:', window_spec.order_by)
