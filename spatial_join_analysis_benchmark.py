import logging
import time
import json
import argparse
from benchmark import mysql_benchmarks, postgresql_benchmarks
from benchmark.benchmark_exception import BenchmarkException
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
# parser.add_argument('--no-index', dest='index', action='store_const', const=False, default=True,
#                    help='Do not create spatial index on datasets')
parser.add_argument('--no-pcs', dest='pcs', action='store_const', const=False, default=True,
                    help='Do not create spatial index on datasets')
parser.add_argument('--parallel', dest='parallel', action='store_const', const=True, default=False,
                    help='Execute queries with multiple threads when possible')
parser.add_argument('--db', dest='db', action='store', default='both',
                    help='Select DB (both/mysql/pg)')
parser.add_argument('--pg-index', dest='pg_index', action='store', default='GIST',
                    help='Select postgis index (GIST/SPGIST/BRIN/NONE)')
parser.add_argument('--mysql-noindex', dest='mysql_index', action='store_const', const=False, default=True,
                    help='Disable MySQL index')
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if args.init:
        logger.info("Initing DB")
        init(create_spatial_index=args.mysql_index, import_gcs=not args.pcs,
             postgis_index=args.pg_index, parallel_query_execution=args.parallel)
    else:
        logger.info("Reusing existing DB")
        start_container()

    mysql_group_name = f"MySQL{' (No Index)' if not args.mysql_index else ''}{ ' (GCS)' if not args.pcs else ''}"
    pg_index_name = 'No' if args.pg_index == 'NONE' else args.pg_index
    postgis_group_name = f"Postgis ({pg_index_name} Index){ ' (GCS)' if not args.pcs else ''}{ ' (Parallel)' if args.parallel else ''}"

    join_benchmarks = []
    if args.db != 'pg':
        join_benchmarks.extend([
            (mysql_group_name, "PointEqualsPoint",
             mysql_benchmarks.PointEqualsPoint(use_projected_crs=args.pcs)),
            (mysql_group_name, "PointIntersectsLine",
             mysql_benchmarks.PointIntersectsLine(use_projected_crs=args.pcs)),
            (mysql_group_name, "PointWithinPolygon",
             mysql_benchmarks.PointWithinPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "LineIntersectsPolygon",
             mysql_benchmarks.LineIntersectsPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "LineWithinPolygon",
             mysql_benchmarks.LineWithinPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "LineIntersectsLine",
             mysql_benchmarks.LineIntersectsLine(use_projected_crs=args.pcs)),
            (mysql_group_name, "PolygonEqualsPolygon",
             mysql_benchmarks.PolygonEqualsPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "PolygonDisjointPolygon",
             mysql_benchmarks.PolygonDisjointPolygon(use_projected_crs=args.pcs, subsampling_factor=10)),
            (mysql_group_name, "PolygonIntersectsPolygon",
             mysql_benchmarks.PolygonIntersectsPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "PolygonWithinPolygon",
             mysql_benchmarks.PolygonWithinPolygon(use_projected_crs=args.pcs)),
        ])
    if args.db != 'mysql':
        join_benchmarks.extend([
            (postgis_group_name, "PointEqualsPoint",
             postgresql_benchmarks.PointEqualsPoint(use_projected_crs=args.pcs)),
            (postgis_group_name, "PointIntersectsLine",
             postgresql_benchmarks.PointIntersectsLine(use_projected_crs=args.pcs)),
            (postgis_group_name, "PointWithinPolygon",
             postgresql_benchmarks.PointWithinPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "LineIntersectsPolygon",
             postgresql_benchmarks.LineIntersectsPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "LineWithinPolygon",
             postgresql_benchmarks.LineWithinPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "LineIntersectsLine",
             postgresql_benchmarks.LineIntersectsLine(use_projected_crs=args.pcs)),
            (postgis_group_name, "PolygonEqualsPolygon",
             postgresql_benchmarks.PolygonEqualsPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "PolygonDisjointPolygon",
             postgresql_benchmarks.PolygonDisjointPolygon(use_projected_crs=args.pcs, subsampling_factor=10)),
            (postgis_group_name, "PolygonIntersectsPolygon",
             postgresql_benchmarks.PolygonIntersectsPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "PolygonWithinPolygon",
             postgresql_benchmarks.PolygonWithinPolygon(use_projected_crs=args.pcs)),
        ])

    analysis_benchmarks = []
    if args.db != 'pg':
        analysis_benchmarks.extend([
            (mysql_group_name, "RetrievePoints",
             mysql_benchmarks.RetrievePoints(use_projected_crs=args.pcs)),
            (mysql_group_name, "LongestLine",
             mysql_benchmarks.LongestLine(use_projected_crs=args.pcs)),
            (mysql_group_name, "TotalLength",
             mysql_benchmarks.TotalLength(use_projected_crs=args.pcs)),
            (mysql_group_name, "RetrieveLines",
             mysql_benchmarks.RetrieveLines(use_projected_crs=args.pcs)),
            (mysql_group_name, "LargestArea",
             mysql_benchmarks.LargestArea(use_projected_crs=args.pcs)),
            (mysql_group_name, "TotalArea",
             mysql_benchmarks.TotalArea(use_projected_crs=args.pcs)),
            (mysql_group_name, "RetrievePolygons",
             mysql_benchmarks.RetrievePolygons(use_projected_crs=args.pcs)),
            (mysql_group_name, "PointNearPoint",
             mysql_benchmarks.PointNearPoint(use_projected_crs=args.pcs)),
            (mysql_group_name, "PointNearPoint2",
             mysql_benchmarks.PointNearPoint2(use_projected_crs=args.pcs)),
            (mysql_group_name, "PointNearLine",
             mysql_benchmarks.PointNearLine(use_projected_crs=args.pcs)),
            (mysql_group_name, "PointNearLine2",
             mysql_benchmarks.PointNearLine2(use_projected_crs=args.pcs)),
            (mysql_group_name, "PointNearPolygon",
             mysql_benchmarks.PointNearPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "SinglePointWithinPolygon",
             mysql_benchmarks.SinglePointWithinPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "LineNearPolygon",
             mysql_benchmarks.LineNearPolygon(use_projected_crs=args.pcs)),
            (mysql_group_name, "SingleLineIntersectsPolygon",
             mysql_benchmarks.SingleLineIntersectsPolygon(use_projected_crs=args.pcs)),
        ])
    if args.db != 'mysql':
        analysis_benchmarks.extend([
            (postgis_group_name, "RetrievePoints",
             postgresql_benchmarks.RetrievePoints(use_projected_crs=args.pcs)),
            (postgis_group_name, "LongestLine",
             postgresql_benchmarks.LongestLine(use_projected_crs=args.pcs)),
            (postgis_group_name, "TotalLength",
             postgresql_benchmarks.TotalLength(use_projected_crs=args.pcs)),
            (postgis_group_name, "RetrieveLines",
             postgresql_benchmarks.RetrieveLines(use_projected_crs=args.pcs)),
            (postgis_group_name, "LargestArea",
             postgresql_benchmarks.LargestArea(use_projected_crs=args.pcs)),
            (postgis_group_name, "TotalArea",
             postgresql_benchmarks.TotalArea(use_projected_crs=args.pcs)),
            (postgis_group_name, "RetrievePolygons",
             postgresql_benchmarks.RetrievePolygons(use_projected_crs=args.pcs)),
            (postgis_group_name, "PointNearPoint",
             postgresql_benchmarks.PointNearPoint(use_projected_crs=args.pcs)),
            (postgis_group_name, "PointNearPoint2",
             postgresql_benchmarks.PointNearPoint2(use_projected_crs=args.pcs)),
            (postgis_group_name, "PointNearLine",
             postgresql_benchmarks.PointNearLine(use_projected_crs=args.pcs)),
            (postgis_group_name, "PointNearLine2",
             postgresql_benchmarks.PointNearLine2(use_projected_crs=args.pcs)),
            (postgis_group_name, "PointNearPolygon",
             postgresql_benchmarks.PointNearPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "SinglePointWithinPolygon",
             postgresql_benchmarks.SinglePointWithinPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "LineNearPolygon",
             postgresql_benchmarks.LineNearPolygon(use_projected_crs=args.pcs)),
            (postgis_group_name, "SingleLineIntersectsPolygon",
             postgresql_benchmarks.SingleLineIntersectsPolygon(use_projected_crs=args.pcs)),
        ])

    benchmarks = []
    if args.mode == 'join':
        benchmarks = join_benchmarks
    elif args.mode == 'analysis':
        benchmarks = analysis_benchmarks

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        if args.db != 'both':
            if args.db == 'mysql' and "MySQL" not in bnchmrk[0]:
                continue
            if args.db == 'pg' and "Postgis" not in bnchmrk[0]:
                continue
        logger.info(f"Starting benchmark {idx+1}")
        try:
            bnchmrk[2].run()
            logger.info(
                f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
            logger.info(
                f"Benchmark average time: {bnchmrk[2].get_average_time()}")
            benchmark_data[bnchmrk[0]][bnchmrk[1]
                                       ] = bnchmrk[2].get_average_time()
            logger.info(f"Result Count: {len(bnchmrk[2].get_results())}")
        except BenchmarkException as e:
            logger.warning(f"Benchmark Exception: {str(e)}")
            benchmark_data[bnchmrk[0]][bnchmrk[1]] = 0

    # Save raw benchmark data to file
    output_file = ""
    if args.mode == 'join':
        output_file = 'join_benchmark'
    elif args.mode == 'analysis':
        output_file = 'analysis_benchmark'
    if not args.mysql_index:
        output_file += '_no_mysql_index'
    output_file += f"_pg_index_{args.pg_index}"
    if not args.pcs:
        output_file += '_gcs'
    if args.parallel:
        output_file += '_parallel'

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
