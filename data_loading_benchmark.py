import logging
import time
import json
import argparse
import docker
from benchmark import mysql_benchmarks, postgresql_benchmarks
from plotting.bar_chart import create_bar_chart
from util.benchmark_helpers import cleanup, start_container
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter
from postgis_docker_wrapper.postgisdockerwrapper import PostgisDockerWrapper
from postgis_docker_wrapper.postgisadapter import PostgisAdapter

"""
Benchmark for loading datasets
"""

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--cleanup', dest='cleanup', action='store_const', const=True, default=False,
                    help='Remove docker containers and volumes')
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    start_container()
    #docker_client = docker.from_env()
    #mysql_docker = MySqlDockerWrapper(docker_client)
    # mysql_docker.start_container()

    # Recreate schema
    mysql_adapter = MySQLAdapter("root", "root-password")
    schema_name = "SpatialDatasets"
    if schema_name in mysql_adapter.get_schemas():
        mysql_adapter.execute(f"DROP SCHEMA {schema_name}")
    mysql_adapter.execute(f"CREATE SCHEMA {schema_name}")

    # Create database for postgis
    docker_client = docker.from_env()
    postgis_docker_wrapper = PostgisDockerWrapper(docker_client)
    schema_name = "spatialdatasets"
    logger.info("Running createdb")
    # Because ogr2ogr can't do CREATE DATABASE for some reason
    logger.info(postgis_docker_wrapper.inject_command(
        f"createdb {schema_name} -h 127.0.0.1 -p 5432 -U postgres -w"))
    logger.info("Schema init done")

    postgis_adapter = PostgisAdapter(
        user="postgres", password="root-password", persist=True)

    logger.info("Loading extension")
    logger.info(postgis_adapter.execute_nontransaction(
        "CREATE EXTENSION IF NOT EXISTS postgis;"))
    logger.info(postgis_adapter.execute_nontransaction(
        "CREATE EXTENSION IF NOT EXISTS postgis_raster;"))
    logger.info(postgis_adapter.execute_nontransaction(
        "CREATE EXTENSION IF NOT EXISTS postgis_topology;"))
    logger.info(postgis_adapter.execute_nontransaction(
        "CREATE EXTENSION IF NOT EXISTS postgis_sfcgal;"))

    benchmarks = [
        ("MySQL", "Airspace (Index)", mysql_benchmarks.LoadAirspaces()),
        ("MySQL", "Airspace (No Index)",
         mysql_benchmarks.LoadAirspaces(with_index=False)),
        ("MySQL", "Airports (Index)", mysql_benchmarks.LoadAirports()),
        ("MySQL", "Airports (No Index)",
         mysql_benchmarks.LoadAirports(with_index=False)),
        ("MySQL", "Routes (Index)", mysql_benchmarks.LoadRoutes()),
        ("MySQL", "Routes (No Index)", mysql_benchmarks.LoadRoutes(with_index=False)),
        # TODO: Woradorn Add benchmarks for postgres without index
        ("Postgis", "Airspace (Index)", postgresql_benchmarks.LoadAirspaces()),
        ("Postgis", "Airports (Index)", postgresql_benchmarks.LoadAirports()),
        ("Postgis", "Routes (Index)", postgresql_benchmarks.LoadRoutes()),
    ]

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
        logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()

    # Save raw benchmark data to file
    with open('results/data_loading_benchmark.json', 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    create_bar_chart(benchmark_data, "Time to Load Dataset",
                     "Seconds", "figures/data_loading_benchmark.png")

    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
