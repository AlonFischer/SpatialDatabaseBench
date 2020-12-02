import logging
import time
import json
import argparse
from benchmark import mysql_benchmarks, postgresql_benchmarks
from benchmark import postgresql_benchmarks
from plotting.bar_chart import create_bar_chart
from util.benchmark_helpers import init, cleanup, start_container

"""
Benchmark for data insertion queries
"""

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--init', dest='init', action='store_const', const=True, default=False,
                    help='Create schemas if necessary and load datasets')
parser.add_argument('--cleanup', dest='cleanup', action='store_const', const=True, default=False,
                    help='Remove docker containers and volumes')
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
        init(create_spatial_index=args.mysql_index, postgis_index=args.pg_index)
    else:
        start_container()

    mysql_group_name = f"MySQL{' (No Index)' if not args.mysql_index else ''}"
    postgis_group_name = f"Postgis{' (No Index)' if args.pg_index == 'NONE' else ''}"

    benchmarks = [
        (mysql_group_name, "Points", mysql_benchmarks.InsertNewPoints()),
        (postgis_group_name, "Points", postgresql_benchmarks.InsertNewPoints()),
        (mysql_group_name, "Lines", mysql_benchmarks.InsertNewLines()),
        (postgis_group_name, "Lines", postgresql_benchmarks.InsertNewLines()),
        (mysql_group_name, "Polygons", mysql_benchmarks.InsertNewPolygons()),
        (postgis_group_name, "Polygons", postgresql_benchmarks.InsertNewPolygons()),
    ]

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        if args.db != 'both':
            if args.db == 'mysql' and bnchmrk[0] != "MySQL":
                continue
            if args.db == 'pg' and bnchmrk[0] != "Postgis":
                continue
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        logger.info(f"Benchmark times: {bnchmrk[2].get_time_measurements()}")
        logger.info(f"Benchmark average time: {bnchmrk[2].get_average_time()}")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = bnchmrk[2].get_average_time()

    # Save raw benchmark data to file
    output_file = "data_insertion_benchmark"
    if not args.mysql_index:
        output_file += '_no_mysql_index'
    output_file += f"_pg_index_{args.pg_index}"
    with open(f"results/{output_file}.json", 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
