import logging
import time
import json
import argparse
from benchmark import mysql_benchmarks, postgresql_benchmarks
from plotting.subsampling_benchmark_graph import create_line_graph
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
        init(create_spatial_index=args.mysql_index,
             import_gcs=not args.pcs, postgis_index=args.pg_index)
    else:
        logger.info("Reusing existing DB")
        start_container()

    subsampling_factors = [1, 2, 4, 8, 16]
    join_benchmarks_template = [
        ("MySQL", "PointEqualsPoint", mysql_benchmarks.PointEqualsPoint),
        ("MySQL", "PolygonEqualsPolygon", mysql_benchmarks.PolygonEqualsPolygon),
        ("MySQL", "PolygonIntersectsPolygon",
         mysql_benchmarks.PolygonIntersectsPolygon),
        ("MySQL", "PolygonWithinPolygon", mysql_benchmarks.PolygonWithinPolygon),
        ("Postgis", "PointEqualsPoint", postgresql_benchmarks.PointEqualsPoint),
        ("Postgis", "PolygonEqualsPolygon",
         postgresql_benchmarks.PolygonEqualsPolygon),
        ("Postgis", "PolygonIntersectsPolygon",
         postgresql_benchmarks.PolygonIntersectsPolygon),
        ("Postgis", "PolygonWithinPolygon",
         postgresql_benchmarks.PolygonWithinPolygon),
    ]

    # join_benchmarks = [
    #    (f"{t[0]}: {t[1]}{group_suffix}", 1/s, t[2](subsampling_factor=s)) for s in subsampling_factors for t in join_benchmarks_template
    # ]

    join_benchmarks = []
    for t in join_benchmarks_template:
        for s in subsampling_factors:
            if t[0] == "MySQL":
                group_suffix = " (No Index)" if not args.mysql_index else ""
                join_benchmarks.append(
                    (f"{t[0]}: {t[1]}{group_suffix}", 1/s, t[2](subsampling_factor=s)))
            else:
                group_suffix = f" ({args.pg_index})"
                join_benchmarks.append(
                    (f"{t[0]}: {t[1]}{group_suffix}", 1/s, t[2](subsampling_factor=s)))

    analysis_benchmarks_template = [
        ("MySQL", "RetrievePoints", mysql_benchmarks.RetrievePoints),
        ("MySQL", "LongestLine", mysql_benchmarks.LongestLine),
        ("MySQL", "PointNearPolygon", mysql_benchmarks.PointNearPolygon),
        ("MySQL", "LineNearPolygon", mysql_benchmarks.LineNearPolygon),
        ("Postgis", "RetrievePoints", postgresql_benchmarks.RetrievePoints),
        ("Postgis", "LongestLine", postgresql_benchmarks.LongestLine),
        ("Postgis", "PointNearPolygon", postgresql_benchmarks.PointNearPolygon),
        ("Postgis", "LineNearPolygon", postgresql_benchmarks.LineNearPolygon),
    ]

    # analysis_benchmarks = [
    #    (f"{t[0]}: {t[1]}{group_suffix}", 1/s, t[2](subsampling_factor=s)) for s in subsampling_factors for t in analysis_benchmarks_template
    # ]

    analysis_benchmarks = []
    for t in analysis_benchmarks_template:
        for s in subsampling_factors:
            if t[0] == "MySQL":
                group_suffix = " (No Index)" if not args.mysql_index else ""
                analysis_benchmarks.append(
                    (f"{t[0]}: {t[1]}{group_suffix}", 1/s, t[2](subsampling_factor=s)))
            else:
                group_suffix = f" ({args.pg_index})"
                analysis_benchmarks.append(
                    (f"{t[0]}: {t[1]}{group_suffix}", 1/s, t[2](subsampling_factor=s)))

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
        output_file = 'subsampling_join_benchmark'
    elif args.mode == 'analysis':
        output_file = 'subsampling_analysis_benchmark'
    if not args.mysql_index:
        output_file += '_no_mysql_index'
    output_file += f"_pg_index_{args.pg_index}"

    with open(f"results/{output_file}.json", 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    create_line_graph(benchmark_data, "Time to Run Query With Different Dataset Sizes", "Size of Dataset Relative to Original",
                      "Seconds", f"figures/{output_file}.png", yscale='linear')

    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
