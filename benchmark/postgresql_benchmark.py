from benchmark.benchmark import Benchmark
import psycopg2
from postgis_docker_wrapper.postgisadapter import PostgisAdapter

"""
PostgreSQL Benchmark Class
"""


class PostgreSQLBenchmark(Benchmark):
    _databaseUri = ''
    _database = 'spatialdatasets'
    adapter_np = None
    adapter_p = None

    def __init__(self, title, repeat_count=7):
        super().__init__(title, repeat_count=repeat_count)
        self.adapter_np = PostgisAdapter(
            "postgres", "root-password", dbname=self._database, persist=False)
        self.adapter_p = PostgisAdapter(
            "postgres", "root-password", dbname=self._database, persist=True)

    def connection(self):
        return PostgreSQLBenchmark._database

    def newConnection(self):
        return psycopg2.connect(PostgreSQLBenchmark._databaseUri)
