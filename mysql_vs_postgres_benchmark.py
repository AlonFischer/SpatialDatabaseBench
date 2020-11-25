import logging
import docker
from benchmark.mysql_benchmarks import *
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
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

    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    gdal_docker_wrapper.import_to_mysql(
        "airspace_3857/Class_Airspace.shp", "airspaces_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "airspace_3857/Airports.shp", "airports_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "routes_3857/ATS_Route.shp", "routes_3857", create_spatial_index=True)

    benchmarks = [
        ("MySQL", "PolygonIntersectsPolygon", PolygonIntersectsPolygon()),
        ("MySQL", "PolygonEqualsPolygon", PolygonEqualsPolygon()),
        ("MySQL", "LineIntersectsLine", LineIntersectsLine())
    ]

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
        logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()
        logger.info(f"Result Count: {len(bnchmrk[2].get_results())}")

    create_bar_chart(benchmark_data, "Time to Run Query",
                     "Seconds", "figures/mysql_vs_postgres_benchmark.png")

    mysql_docker.stop_container()
    mysql_docker.remove_container()


if __name__ == "__main__":
    main()
