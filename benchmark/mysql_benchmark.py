from benchmark.benchmark import Benchmark
from mysqlutils.mysqladapter import MySQLAdapter


class MysqlBenchmark(Benchmark):
    """Abstract parent class for mysql benchmarks"""

    def __init__(self, adapter, title, repeat_count=3):
        super().__init__(title, repeat_count=repeat_count)
        self.adapter = adapter
