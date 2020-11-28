import logging
import time
import json
import argparse
from benchmark import mysql_benchmarks
from benchmark import postgresql_benchmarks
from plotting.bar_chart import create_bar_chart
from util.benchmark_helpers import init, cleanup

"""
Benchmark for data insertion queries
"""

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('mode', metavar='M', type=str,
                    choices=['join', 'analysis'],
                    help='Constrains which benchmarks are run')
parser.add_argument('--init', dest='init', action='store_true',
                    help='Create schemas if necessary and load datasets')
parser.add_argument('--no-init', dest='init', action='store_false',
                    help='Do not create schemas and load datasets')
parser.add_argument('--cleanup', dest='cleanup', action='store_true',
                    help='Remove docker containers and volumes')
parser.add_argument('--cleanup', dest='cleanup', action='store_false',
                    help='Do not remove docker containers and volumes')
parser.set_defaults(init=True)
parser.set_defaults(cleanup=True)
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if args.init:
        init()

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

    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
