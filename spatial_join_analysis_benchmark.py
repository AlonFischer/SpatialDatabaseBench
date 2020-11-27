import logging
import docker
import time
import json
import argparse
from benchmark import mysql_benchmarks
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
from plotting.bar_chart import create_bar_chart

"""
Main benchmarking script
"""

args = None


def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    start = time.perf_counter()

    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)
    mysql_docker.start_container()

    # Create schema
    mysql_adapter = MySQLAdapter("root", "root-password")
    mysql_adapter.execute(f"CREATE SCHEMA SpatialDatasets")

    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    gdal_docker_wrapper.import_to_mysql(
        "airspace_3857/Class_Airspace.shp", "airspaces_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "airports_3857/Airports.shp", "airports_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "routes_3857/ATS_Route.shp", "routes_3857", create_spatial_index=True)

    join_benchmarks = [
        ("MySQL", "PointEqualsPoint", mysql_benchmarks.PointEqualsPoint()),
        ("MySQL", "PointIntersectsLine", mysql_benchmarks.PointIntersectsLine()),
        ("MySQL", "PointWithinPolygon", mysql_benchmarks.PointWithinPolygon()),
        ("MySQL", "LineIntersectsPolygon", mysql_benchmarks.LineIntersectsPolygon()),
        ("MySQL", "LineWithinPolygon", mysql_benchmarks.LineWithinPolygon()),
        ("MySQL", "LineIntersectsLine", mysql_benchmarks.LineIntersectsLine()),
        ("MySQL", "PolygonEqualsPolygon", mysql_benchmarks.PolygonEqualsPolygon()),
        ("MySQL", "PolygonDisjointPolygon",
         mysql_benchmarks.PolygonDisjointPolygon(subsampling_factor=10)),
        ("MySQL", "PolygonIntersectsPolygon",
         mysql_benchmarks.PolygonIntersectsPolygon()),
        ("MySQL", "PolygonWithinPolygon", mysql_benchmarks.PolygonWithinPolygon()),
    ]

    analysis_benchmarks = [
        ("MySQL", "RetrievePoints", mysql_benchmarks.RetrievePoints()),
        ("MySQL", "LongestLine", mysql_benchmarks.LongestLine()),
        ("MySQL", "TotalLength", mysql_benchmarks.TotalLength()),
        ("MySQL", "RetrieveLines", mysql_benchmarks.RetrieveLines()),
        ("MySQL", "LargestArea", mysql_benchmarks.LargestArea()),
        ("MySQL", "TotalArea", mysql_benchmarks.TotalArea()),
        ("MySQL", "RetrievePolygons", mysql_benchmarks.RetrievePolygons()),
        ("MySQL", "PointNearPoint", mysql_benchmarks.PointNearPoint()),
        ("MySQL", "PointNearPoint2", mysql_benchmarks.PointNearPoint2()),
        ("MySQL", "PointNearLine", mysql_benchmarks.PointNearLine()),
        ("MySQL", "PointNearLine2", mysql_benchmarks.PointNearLine2()),
        ("MySQL", "PointNearPolygon", mysql_benchmarks.PointNearPolygon()),
        ("MySQL", "SinglePointWithinPolygon",
         mysql_benchmarks.SinglePointWithinPolygon()),
        ("MySQL", "LineNearPolygon", mysql_benchmarks.LineNearPolygon()),
        ("MySQL", "SingleLineIntersectsPolygon",
         mysql_benchmarks.SingleLineIntersectsPolygon()),
    ]

    benchmarks = []
    if args.mode == 'join':
        benchmarks = join_benchmarks
    elif args.mode == 'analysis':
        benchmarks = analysis_benchmarks

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
        logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()
        logger.info(f"Result Count: {len(bnchmrk[2].get_results())}")

    # Save raw benchmark data to file
    output_file = ""
    if args.mode == 'join':
        output_file = 'join_benchmark'
    elif args.mode == 'analysis':
        output_file = 'analysis_benchmark'

    with open(f"results/{output_file}.json", 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    create_bar_chart(benchmark_data, "Time to Run Query",
                     "Seconds", f"figures/{output_file}.png", yscale='log')

    mysql_docker.stop_container()
    mysql_docker.remove_container()
    mysql_docker.remove_volume()

    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('mode', metavar='M', type=str,
                        choices=['join', 'analysis'],
                        help='Constrains which benchmarks are run')
    args = parser.parse_args()

    main()
