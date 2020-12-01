#!/usr/bin/env python3
"""
PostgreSQL Benchmark class
"""
__author__ = "Stephane Carrez"
__copyright__ = "Copyright (C) 2018 Stephane Carrez"
__license__ = 'Apache License, Version 2.0'

from benchmark.benchmark import Benchmark
import psycopg2
from postgis_docker_wrapper.postgisadapter import PostgisAdapter

"""
PostgreSQL Benchmark
"""
class PostgreSQLBenchmark(Benchmark):
    _databaseUri = ''
    _database = 'spatialdatasets'
    adapter_np = None
    adapter_p = None

    def __init__(self, title, repeat_count=3):
        super().__init__(title, repeat_count=repeat_count)
        self.adapter_np = PostgisAdapter("postgres", "root-password", dbname=self._database, persist=False)
        self.adapter_p = PostgisAdapter("postgres", "root-password", dbname=self._database, persist=True)

    @staticmethod
    def setup():
        database = "spatialdatasets"
        if not database:
            raise Exception("Missing 'postgresql.database' configuration")
        if database.startswith("jdbc:sqlite:"):
            database = database[12:]
        Benchmark._driver = 'postgresql'
        PostgreSQLBenchmark._databaseUri = database
        PostgreSQLBenchmark._database = psycopg2.connect(database)

    def connection(self):
        return PostgreSQLBenchmark._database

    def newConnection(self):
        return psycopg2.connect(PostgreSQLBenchmark._databaseUri)
