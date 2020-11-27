import logging
import docker
import time
import json
from benchmark import mysql_benchmarks
from benchmark import postgresql_benchmarks
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

    start = time.perf_counter()

    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)
    mysql_docker.start_container()

    mysql_adapter = MySQLAdapter("root", "root-password")
    mysql_adapter.execute(f"CREATE SCHEMA SpatialDatasets")

    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    gdal_docker_wrapper.import_to_mysql(
        "airspace_3857/Class_Airspace.shp", "airspaces_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "airports_3857/Airports.shp", "airports_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "routes_3857/ATS_Route.shp", "routes_3857", create_spatial_index=True)

    benchmarks = [
        ("MySQL", "Points", mysql_benchmarks.InsertNewPoints()),
        ("MySQL", "Lines", mysql_benchmarks.InsertNewLines()),
        ("MySQL", "Polygons", mysql_benchmarks.InsertNewPolygons()),
    ]

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
        logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()

    # Save raw benchmark data to file
    with open('results/data_insertion_benchmark.json', 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    create_bar_chart(benchmark_data, "Time to Run Query",
                     "Seconds", "figures/data_insertion_benchmark.png")

    mysql_docker.stop_container()
    mysql_docker.remove_container()
    mysql_docker.remove_volume()

    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")


if __name__ == "__main__":
    main()
