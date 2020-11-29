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

def check(bnchmrk, mysql_results, postgres_results):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    compare_results = list(set(mysql_results) | set(postgres_results))
    if len(compare_results) == len(mysql_results):
        logger.info(f"{bnchmrk} integrity: TRUE")
    else:
        diff_results = list((set(mysql_results) - set(postgres_results)) | (set(postgres_results) - set(mysql_results)))
        logger.info(f"{bnchmrk} integrity: FALSE on {diff_results}")

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

    #gdal_docker_wrapper = GdalDockerWrapper(docker_client)
    #gdal_docker_wrapper.import_to_mysql(
    #    "airspace_3857/Class_Airspace.shp", "airspaces_3857", create_spatial_index=True)
    #gdal_docker_wrapper.import_to_mysql(
    #    "airports_3857/Airports.shp", "airports_3857", create_spatial_index=True)
    #gdal_docker_wrapper.import_to_mysql(
    #    "routes_3857/ATS_Route.shp", "routes_3857", create_spatial_index=True)

    # Create postgres schema

    #TODO insert postgres queries, as benchmarks[][1]
    benchmarks = {
        "PointEqualsPoint": [mysql_benchmarks.PointEqualsPoint(), mysql_benchmarks.PointEqualsPoint()],
        "PointIntersectsLine": [mysql_benchmarks.PointIntersectsLine(), mysql_benchmarks.PointIntersectsLine()],
        "PointWithinPolygon": [mysql_benchmarks.PointWithinPolygon(), mysql_benchmarks.PointWithinPolygon()],
        "LineIntersectsPolygon": [mysql_benchmarks.LineIntersectsPolygon(), mysql_benchmarks.LineIntersectsPolygon()],
        "LineWithinPolygon": [mysql_benchmarks.LineWithinPolygon(), mysql_benchmarks.LineWithinPolygon()],
        "LineIntersectsLine": [mysql_benchmarks.LineIntersectsLine(), mysql_benchmarks.LineIntersectsLine()],
        "PolygonEqualsPolygon": [mysql_benchmarks.PolygonEqualsPolygon(), mysql_benchmarks.PolygonEqualsPolygon()],
        "PolygonDisjointPolygon": [mysql_benchmarks.PolygonDisjointPolygon(subsampling_factor=10), mysql_benchmarks.PolygonDisjointPolygon(subsampling_factor=10)],
        "PolygonIntersectsPolygon": [mysql_benchmarks.PolygonIntersectsPolygon(), mysql_benchmarks.PolygonIntersectsPolygon()],
        "PolygonWithinPolygon": [mysql_benchmarks.PolygonWithinPolygon(), mysql_benchmarks.PolygonWithinPolygon()],
        "RetrievePoints": [mysql_benchmarks.RetrievePoints(), mysql_benchmarks.RetrievePoints()],
        "LongestLine": [mysql_benchmarks.LongestLine(), mysql_benchmarks.LongestLine()],
        "TotalLength": [mysql_benchmarks.TotalLength(), mysql_benchmarks.TotalLength()],
        "RetrieveLines": [mysql_benchmarks.RetrieveLines(), mysql_benchmarks.RetrieveLines()],
        "LargestArea": [mysql_benchmarks.LargestArea(), mysql_benchmarks.LargestArea()],
        "TotalArea": [mysql_benchmarks.TotalArea(), mysql_benchmarks.TotalArea()],
        "RetrievePolygons": [mysql_benchmarks.RetrievePolygons(), mysql_benchmarks.RetrievePolygons()],
        "PointNearPoint": [mysql_benchmarks.PointNearPoint(), mysql_benchmarks.PointNearPoint()],
        "PointNearPoint2": [mysql_benchmarks.PointNearPoint2(), mysql_benchmarks.PointNearPoint2()],
        "PointNearLine": [mysql_benchmarks.PointNearLine(), mysql_benchmarks.PointNearLine()],
        "PointNearLine2": [mysql_benchmarks.PointNearLine2(), mysql_benchmarks.PointNearLine2()],
        "PointNearPolygon": [mysql_benchmarks.PointNearPolygon(), mysql_benchmarks.PointNearPolygon()],
        "SinglePointWithinPolygon": [mysql_benchmarks.SinglePointWithinPolygon(), mysql_benchmarks.SinglePointWithinPolygon()],
        "LineNearPolygon": [mysql_benchmarks.LineNearPolygon(), mysql_benchmarks.LineNearPolygon()],
        "SingleLineIntersectsPolygon": [mysql_benchmarks.SingleLineIntersectsPolygon(), mysql_benchmarks.SingleLineIntersectsPolygon()],
    }

    # test all arg
    if len(sys.argv) > 1 and sys.argv[1] == "all":
        for bnchmrk in benchmarks:
            logger.info(f"Starting benchmark {bnchmrk}")
            mysql_results = benchmarks[bnchmrk][0].execute()
            postgres_results = benchmarks[bnchmrk][1].execute()
            check(bnchmrk, mysql_results, postgres_results)
    
    #test single arg
    elif len(sys.argv) > 1 and benchmarks[sys.argv[1]] != None: 
        logger.info(f"Starting Integrity Check for {sys.argv[1]}")
        mysql_results = benchmarks[sys.argv[1]][0].execute()
        postgres_results = benchmarks[sys.argv[1]][1].execute()
        check(sys.argv[1], mysql_results, postgres_results)

    else:
        logger.info(f"Query does not exist")

    #Cleanup
    #mysql_docker.stop_container()
    #mysql_docker.remove_container()
    #mysql_docker.remove_volume()
    #TODO cleanup postgres docker stuff

if __name__ == "__main__":

    main()
