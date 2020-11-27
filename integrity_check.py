import logging
import docker
import json
import sys
import argparse
from benchmark import mysql_benchmarks
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
#TODO import postgres stuff

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    docker_client = docker.from_env()
    mysql_docker = MySqlDockerWrapper(docker_client)
    mysql_docker.start_container()

    # For debugging
    # mysql_docker.stop_container()
    # mysql_docker.remove_container()
    # mysql_docker.remove_volume()

    #mysql_docker.start_container()
    # Create mysql schema
    mysql_adapter = MySQLAdapter("root", "root-password")
    #mysql_adapter.execute(f"CREATE SCHEMA SpatialDatasets")

    gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    gdal_docker_wrapper.import_to_mysql(
        "airspace_3857/Class_Airspace.shp", "airspaces_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "airports_3857/Airports.shp", "airports_3857", create_spatial_index=True)
    gdal_docker_wrapper.import_to_mysql(
        "routes_3857/ATS_Route.shp", "routes_3857", create_spatial_index=True)

    # Create postgres schema

    #TODO insert postgres queries, as benchmarks[][1]
    benchmarks = {
        "PointEqualsPoint": [mysql_benchmarks.PointEqualsPoint()],
        "PointIntersectsLine": [mysql_benchmarks.PointIntersectsLine()],
        "PointWithinPolygon": [mysql_benchmarks.PointWithinPolygon()],
        "LineIntersectsPolygon": [mysql_benchmarks.LineIntersectsPolygon()],
        "LineWithinPolygon": [mysql_benchmarks.LineWithinPolygon()],
        "LineIntersectsLine": [mysql_benchmarks.LineIntersectsLine()],
        "PolygonEqualsPolygon": [mysql_benchmarks.PolygonEqualsPolygon()],
        "PolygonDisjointPolygon": [mysql_benchmarks.PolygonDisjointPolygon(subsampling_factor=10)],
        "PolygonIntersectsPolygon": [mysql_benchmarks.PolygonIntersectsPolygon()],
        "PolygonWithinPolygon": [mysql_benchmarks.PolygonWithinPolygon()],
        "RetrievePoints": [mysql_benchmarks.RetrievePoints()],
        "LongestLine": [mysql_benchmarks.LongestLine()],
        "TotalLength": [mysql_benchmarks.TotalLength()],
        "RetrieveLines": [mysql_benchmarks.RetrieveLines()],
        "LargestArea": [mysql_benchmarks.LargestArea()],
        "TotalArea": [mysql_benchmarks.TotalArea()],
        "RetrievePolygons": [mysql_benchmarks.RetrievePolygons()],
        "PointNearPoint": [mysql_benchmarks.PointNearPoint()],
        "PointNearPoint2": [mysql_benchmarks.PointNearPoint2()],
        "PointNearLine": [mysql_benchmarks.PointNearLine()],
        "PointNearLine2": [mysql_benchmarks.PointNearLine2()],
        "PointNearPolygon": [mysql_benchmarks.PointNearPolygon()],
        "SinglePointWithinPolygon": [mysql_benchmarks.SinglePointWithinPolygon()],
        "LineNearPolygon": [mysql_benchmarks.LineNearPolygon()],
        "SingleLineIntersectsPolygon": [mysql_benchmarks.SingleLineIntersectsPolygon()],
    }

    # TODO - test all arg
    # benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    # for idx, bnchmrk in enumerate(benchmarks):
    #     logger.info(f"Starting benchmark {idx+1}")
    #     bnchmrk[2].run()
    #     logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
    #     logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
    #     benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()
    #     logger.info(f"Result Count: {len(bnchmrk[2].get_results())}")

    # TODO - test single arg
    logger.info(f"Starting Integrity Check for {sys.argv[1]}")
    mysql_results = benchmarks[sys.argv[1]][0].execute()
    logger.info(f"mysql results: {mysql_results}")
    #postgres_results = benchmark[sys.argv[0]][1]

    #TODO - result comparison
    #compare_results = mysql_results | postgres_results
    # if compare_results.length == mysql_results.length
    #     logger.info(f"{sys.argv[1]} integrity: TRUE")
    # else
    #     diff_results = (mysql_results - postgres_results) | (postgres_results - mysql_results)
    #     logger.info(f"{sys.argv[1]} integrity: FALSE on {diff_results}")



    #Cleanup
    mysql_docker.stop_container()
    mysql_docker.remove_container()
    mysql_docker.remove_volume()
    #TODO cleanup postgres docker stuff

if __name__ == "__main__":

    main()
