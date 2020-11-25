import logging
import docker
from benchmark.mysql_benchmarks import *
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
from plotting.bar_chart import create_bar_chart

"""
Main benchmarking script
"""


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)
    mysql_docker.start_container()

    # Recreate schema
    mysql_adapter = MySQLAdapter("root", "root-password")
    schema_name = "SpatialDatasets"
    if schema_name in mysql_adapter.get_schemas():
        mysql_adapter.execute(f"DROP SCHEMA {schema_name}")
    mysql_adapter.execute(f"CREATE SCHEMA {schema_name}")

    benchmarks = [
        ("MySQL", "Airspace (Index)", LoadAirspaces()),
        ("MySQL", "Airspace (No Index)", LoadAirspaces(with_index=False)),
        ("MySQL", "Airports (Index)", LoadAirports()),
        ("MySQL", "Airports (No Index)", LoadAirports(with_index=False)),
        ("MySQL", "Routes (Index)", LoadRoutes()),
        ("MySQL", "Routes (No Index)", LoadRoutes(with_index=False)),
    ]

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
        logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()

    create_bar_chart(benchmark_data, "Time to Load Dataset",
                     "Seconds", "figures/data_loading_benchmark.png")

    mysql_docker.stop_container()
    mysql_docker.remove_container()


if __name__ == "__main__":
    main()
