#!/usr/bin/env python3
"""
PostgreSQL Benchmark tests
"""
__author__ = "Stephane Carrez"
__copyright__ = "Copyright (C) 2018 Stephane Carrez"
__license__ = 'Apache License, Version 2.0'

import psycopg2
import docker
from benchmark.postgresql_benchmark import PostgreSQLBenchmark
from postgis_docker_wrapper.postgisadapter import PostgisAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
import logging


class LoadAirspaces(PostgreSQLBenchmark):
    _logger = logging.getLogger(__name__)
    _title = "Load Airspaces"
    _table_name = "airspaces_temp"

    def __init__(self, with_index=True):
        super().__init__("LoadAirspaces", 1)
        docker_client = docker.from_env()
        self.gdal_docker_wrapper = GdalDockerWrapper(docker_client)
        self.with_index = with_index

    def execute(self):
        self.gdal_docker_wrapper.import_to_postgis(
            "airspace/Class_Airspace.shp", LoadAirspaces._table_name)
        print("Load done")

    def cleanup(self):
        print("Cleanup called")
        self._logger.info(self.adapter_p.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"))

        self.adapter_p.execute(
            f"DROP TABLE {LoadAirspaces._table_name}")


