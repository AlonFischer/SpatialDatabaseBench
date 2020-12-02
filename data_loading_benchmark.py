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
parser.add_argument('--db', dest='db', action='store', default='both',
                    help='Select DB (both/mysql/pg)')
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
        ("MySQL (Index)", "Airspace", mysql_benchmarks.LoadAirspaces()),
        ("MySQL (No Index)", "Airspace",
         mysql_benchmarks.LoadAirspaces(with_index=False)),
        ("MySQL (Index)", "Airports", mysql_benchmarks.LoadAirports()),
        ("MySQL (No Index)", "Airports",
         mysql_benchmarks.LoadAirports(with_index=False)),
        ("MySQL (Index)", "Routes", mysql_benchmarks.LoadRoutes()),
        ("MySQL (No Index)", "Routes", mysql_benchmarks.LoadRoutes(with_index=False)),
        # Add benchmarks for postgres without index
        ("Postgis (GIST Index)", "Airspace",
         postgresql_benchmarks.LoadAirspaces(with_index="GIST")),
        ("Postgis (SPGIST Index)", "Airspace",
         postgresql_benchmarks.LoadAirspaces(with_index="SPGIST")),
        ("Postgis (BRIN Index)", "Airspace",
         postgresql_benchmarks.LoadAirspaces(with_index="BRIN")),
        ("Postgis (No Index)", "Airspace",
         postgresql_benchmarks.LoadAirspaces(with_index="NONE")),
        ("Postgis (GIST Index)", "Airports",
         postgresql_benchmarks.LoadAirports(with_index="GIST")),
        ("Postgis (SPGIST Index)", "Airports",
         postgresql_benchmarks.LoadAirports(with_index="SPGIST")),
        ("Postgis (BRIN Index)", "Airports",
         postgresql_benchmarks.LoadAirports(with_index="BRIN")),
        ("Postgis (No Index)", "Airports",
         postgresql_benchmarks.LoadAirports(with_index="NONE")),
        ("Postgis (GIST Index)", "Routes",
         postgresql_benchmarks.LoadRoutes(with_index="GIST")),
        ("Postgis (SPGIST Index)", "Routes",
         postgresql_benchmarks.LoadRoutes(with_index="SPGIST")),
        ("Postgis (BRIN Index)", "Routes",
         postgresql_benchmarks.LoadRoutes(with_index="BRIN")),
        ("Postgis (No Index)", "Routes",
         postgresql_benchmarks.LoadRoutes(with_index="NONE")),
    ]

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        if args.db != 'both':
            if args.db == 'mysql' and bnchmrk[0] != "MySQL":
                continue
            if args.db == 'pg' and bnchmrk[0] != "Postgis":
                continue
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
        logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()

    # Save raw benchmark data to file
    with open('results/data_loading_benchmark.json', 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    create_bar_chart(benchmark_data, "Time to Load Dataset",
                     "Seconds", "figures/data_loading_benchmark.png", yscale='log')

    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
