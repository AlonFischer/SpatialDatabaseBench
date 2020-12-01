import logging
import docker
import json
import sys
import argparse
from pprint import pprint
from benchmark import mysql_benchmarks, postgresql_benchmarks
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
from util.benchmark_helpers import start_container, init
#TODO import postgres stuff

def check(bnchmrk, mysql_results, postgres_results):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    compare_results = list(set(mysql_results) | set(postgres_results))
    if len(compare_results) == len(mysql_results):
        logger.info(f"{bnchmrk} integrity: OK")
        return None
    else:
        diff_results = list((set(mysql_results) - set(postgres_results)) | (set(postgres_results) - set(mysql_results)))
        logger.info(f"{bnchmrk} integrity: FAIL on {diff_results}")
        return (bnchmrk, diff_results)
    

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    #docker_client = docker.from_env()
    #mysql_docker = MySqlDockerWrapper(docker_client)
    #mysql_docker.start_container()



    # For debugging
    # mysql_docker.stop_container()
    # mysql_docker.remove_container()
    # mysql_docker.remove_volume()

    #mysql_docker.start_container()

    # Create mysql schema

    if True:
        init()
    else:
        start_container()

    # Create postgres schema

    #TODO insert postgres queries, as benchmarks[][1]
    benchmarks = {
        "PointEqualsPoint": [mysql_benchmarks.PointEqualsPoint(), postgresql_benchmarks.PointEqualsPoint()],
        "PointIntersectsLine": [mysql_benchmarks.PointIntersectsLine(), postgresql_benchmarks.PointIntersectsLine()],
        "PointWithinPolygon": [mysql_benchmarks.PointWithinPolygon(), postgresql_benchmarks.PointWithinPolygon()],
        "LineIntersectsPolygon": [mysql_benchmarks.LineIntersectsPolygon(), postgresql_benchmarks.LineIntersectsPolygon()],
        "LineWithinPolygon": [mysql_benchmarks.LineWithinPolygon(), postgresql_benchmarks.LineWithinPolygon()],
        "LineIntersectsLine": [mysql_benchmarks.LineIntersectsLine(), postgresql_benchmarks.LineIntersectsLine()],
        "PolygonEqualsPolygon": [mysql_benchmarks.PolygonEqualsPolygon(), postgresql_benchmarks.PolygonEqualsPolygon()],
        "PolygonDisjointPolygon": [mysql_benchmarks.PolygonDisjointPolygon(subsampling_factor=10), postgresql_benchmarks.PolygonDisjointPolygon(subsampling_factor=10)],
        "PolygonIntersectsPolygon": [mysql_benchmarks.PolygonIntersectsPolygon(), postgresql_benchmarks.PolygonIntersectsPolygon()],
        "PolygonWithinPolygon": [mysql_benchmarks.PolygonWithinPolygon(), postgresql_benchmarks.PolygonWithinPolygon()],
        "RetrievePoints": [mysql_benchmarks.RetrievePoints(), postgresql_benchmarks.RetrievePoints()],
        "LongestLine": [mysql_benchmarks.LongestLine(), postgresql_benchmarks.LongestLine()],
        "TotalLength": [mysql_benchmarks.TotalLength(), postgresql_benchmarks.TotalLength()],
        "RetrieveLines": [mysql_benchmarks.RetrieveLines(), postgresql_benchmarks.RetrieveLines()],
        "LargestArea": [mysql_benchmarks.LargestArea(), postgresql_benchmarks.LargestArea()],
        "TotalArea": [mysql_benchmarks.TotalArea(), postgresql_benchmarks.TotalArea()],
        "RetrievePolygons": [mysql_benchmarks.RetrievePolygons(), postgresql_benchmarks.RetrievePolygons()],
        "PointNearPoint": [mysql_benchmarks.PointNearPoint(), postgresql_benchmarks.PointNearPoint()],
        "PointNearPoint2": [mysql_benchmarks.PointNearPoint2(), postgresql_benchmarks.PointNearPoint2()],
        "PointNearLine": [mysql_benchmarks.PointNearLine(), postgresql_benchmarks.PointNearLine()],
        "PointNearLine2": [mysql_benchmarks.PointNearLine2(), postgresql_benchmarks.PointNearLine2()],
        "PointNearPolygon": [mysql_benchmarks.PointNearPolygon(), postgresql_benchmarks.PointNearPolygon()],
        "SinglePointWithinPolygon": [mysql_benchmarks.SinglePointWithinPolygon(), postgresql_benchmarks.SinglePointWithinPolygon()],
        "LineNearPolygon": [mysql_benchmarks.LineNearPolygon(), postgresql_benchmarks.LineNearPolygon()],
        "SingleLineIntersectsPolygon": [mysql_benchmarks.SingleLineIntersectsPolygon(), postgresql_benchmarks.SingleLineIntersectsPolygon()],
    }

    # test all arg
    if len(sys.argv) > 1 and sys.argv[1] == "all":
        fails = []
        for bnchmrk in benchmarks:
            logger.info(f"Starting benchmark {bnchmrk}")
            mysql_results = benchmarks[bnchmrk][0].execute()
            postgres_results = benchmarks[bnchmrk][1].execute()
            retval = check(bnchmrk, mysql_results, postgres_results)
            if retval is not None:
                fails.append(retval)
        print("---fails--")
        pprint(fails)
    
    #test single arg
    elif len(sys.argv) > 1 and benchmarks[sys.argv[1]] != None: 
        logger.info(f"Starting Integrity Check for {sys.argv[1]}")
        mysql_results = benchmarks[sys.argv[1]][0].execute()
        postgres_results = benchmarks[sys.argv[1]][1].execute()
        check(sys.argv[1], mysql_results, postgres_results)

    else:
        logger.info(f"Please specify target query/queries or 'all'")

    #Cleanup
    #mysql_docker.stop_container()
    #mysql_docker.remove_container()
    #mysql_docker.remove_volume()
    #TODO cleanup postgres docker stuff

if __name__ == "__main__":

    main()
