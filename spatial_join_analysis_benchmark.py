import logging
import time
import json
import argparse
from benchmark import mysql_benchmarks
from benchmark import postgresql_benchmarks
from mysqlutils.mysqldockerwrapper import MySqlDockerWrapper
from mysqlutils.mysqladapter import MySQLAdapter
from gdal.gdaldockerwrapper import GdalDockerWrapper
from plotting.bar_chart import create_bar_chart
from util.benchmark_helpers import init, cleanup, start_container

"""
Benchmark for spatial join and analysis queries
"""

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('mode', metavar='M', type=str,
                    choices=['join', 'analysis'],
                    help='Constrains which benchmarks are run')
parser.add_argument('--init', dest='init', action='store_const', const=True, default=False,
                    help='Create schemas if necessary and load datasets')
parser.add_argument('--cleanup', dest='cleanup', action='store_const', const=True, default=False,
                    help='Remove docker containers and volumes')
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if args.init:
        logger.info("Initing DB")
        init()
    else:
        logger.info("Reusing existing DB")
        start_container()

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
        ("Postgis", "PointEqualsPoint", postgresql_benchmarks.PointEqualsPoint()),
        ("Postgis", "PointIntersectsLine", postgresql_benchmarks.PointIntersectsLine()),
        ("Postgis", "PointWithinPolygon", postgresql_benchmarks.PointWithinPolygon()),
        ("Postgis", "LineIntersectsPolygon", postgresql_benchmarks.LineIntersectsPolygon()),
        ("Postgis", "LineWithinPolygon", postgresql_benchmarks.LineWithinPolygon()),
        ("Postgis", "LineIntersectsLine", postgresql_benchmarks.LineIntersectsLine()),
        ("Postgis", "PolygonEqualsPolygon", postgresql_benchmarks.PolygonEqualsPolygon()),
        ("Postgis", "PolygonDisjointPolygon",
         postgresql_benchmarks.PolygonDisjointPolygon(subsampling_factor=10)),
        ("Postgis", "PolygonIntersectsPolygon",
         postgresql_benchmarks.PolygonIntersectsPolygon()),
        ("Postgis", "PolygonWithinPolygon", postgresql_benchmarks.PolygonWithinPolygon()),
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
        ("Postgis", "RetrievePoints", postgresql_benchmarks.RetrievePoints()),
        ("Postgis", "LongestLine", postgresql_benchmarks.LongestLine()),
        ("Postgis", "TotalLength", postgresql_benchmarks.TotalLength()),
        ("Postgis", "RetrieveLines", postgresql_benchmarks.RetrieveLines()),
        ("Postgis", "LargestArea", postgresql_benchmarks.LargestArea()),
        ("Postgis", "TotalArea", postgresql_benchmarks.TotalArea()),
        ("Postgis", "RetrievePolygons", postgresql_benchmarks.RetrievePolygons()),
        ("Postgis", "PointNearPoint", postgresql_benchmarks.PointNearPoint()),
        ("Postgis", "PointNearPoint2", postgresql_benchmarks.PointNearPoint2()),
        ("Postgis", "PointNearLine", postgresql_benchmarks.PointNearLine()),
        ("Postgis", "PointNearLine2", postgresql_benchmarks.PointNearLine2()),
        ("Postgis", "PointNearPolygon", postgresql_benchmarks.PointNearPolygon()),
        ("Postgis", "SinglePointWithinPolygon",
         postgresql_benchmarks.SinglePointWithinPolygon()),
        ("Postgis", "LineNearPolygon", postgresql_benchmarks.LineNearPolygon()),
        ("Postgis", "SingleLineIntersectsPolygon",
         postgresql_benchmarks.SingleLineIntersectsPolygon()),
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

    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
