import logging
import time
import json
import argparse
from benchmark import mysql_benchmarks
from plotting.subsampling_benchmark_graph import create_line_graph
from util.benchmark_helpers import init, cleanup

"""
Benchmark for spatial join and analysis queries
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
parser.add_argument('--no-cleanup', dest='cleanup', action='store_false',
                    help='Do not remove docker containers and volumes')
parser.set_defaults(init=True)
parser.set_defaults(cleanup=True)
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    if args.init:
        init()

    subsampling_factors = [1, 2, 4, 8, 16]
    join_benchmarks_template = [
        ("MySQL", "PointEqualsPoint", mysql_benchmarks.PointEqualsPoint),
        ("MySQL", "PointIntersectsLine", mysql_benchmarks.PointIntersectsLine),
        ("MySQL", "PointWithinPolygon", mysql_benchmarks.PointWithinPolygon),
        ("MySQL", "LineIntersectsPolygon", mysql_benchmarks.LineIntersectsPolygon),
        ("MySQL", "LineWithinPolygon", mysql_benchmarks.LineWithinPolygon),
        ("MySQL", "LineIntersectsLine", mysql_benchmarks.LineIntersectsLine),
        ("MySQL", "PolygonEqualsPolygon", mysql_benchmarks.PolygonEqualsPolygon),
        ("MySQL", "PolygonIntersectsPolygon",
         mysql_benchmarks.PolygonIntersectsPolygon),
        ("MySQL", "PolygonWithinPolygon", mysql_benchmarks.PolygonWithinPolygon),
    ]

    join_benchmarks = [
        (f"{t[0]}: {t[1]}", 1/s, t[2](subsampling_factor=s)) for s in subsampling_factors for t in join_benchmarks_template
    ]

    analysis_benchmarks = [
        # ("MySQL", "RetrievePoints", mysql_benchmarks.RetrievePoints()),
        # ("MySQL", "LongestLine", mysql_benchmarks.LongestLine()),
        # ("MySQL", "TotalLength", mysql_benchmarks.TotalLength()),
        # ("MySQL", "RetrieveLines", mysql_benchmarks.RetrieveLines()),
        # ("MySQL", "LargestArea", mysql_benchmarks.LargestArea()),
        # ("MySQL", "TotalArea", mysql_benchmarks.TotalArea()),
        # ("MySQL", "RetrievePolygons", mysql_benchmarks.RetrievePolygons()),
        # ("MySQL", "PointNearPoint", mysql_benchmarks.PointNearPoint()),
        # ("MySQL", "PointNearPoint2", mysql_benchmarks.PointNearPoint2()),
        # ("MySQL", "PointNearLine", mysql_benchmarks.PointNearLine()),
        # ("MySQL", "PointNearLine2", mysql_benchmarks.PointNearLine2()),
        # ("MySQL", "PointNearPolygon", mysql_benchmarks.PointNearPolygon()),
        # ("MySQL", "SinglePointWithinPolygon",
        #  mysql_benchmarks.SinglePointWithinPolygon()),
        # ("MySQL", "LineNearPolygon", mysql_benchmarks.LineNearPolygon()),
        # ("MySQL", "SingleLineIntersectsPolygon",
        #  mysql_benchmarks.SingleLineIntersectsPolygon()),
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
        output_file = 'subsampling_join_benchmark'
    elif args.mode == 'analysis':
        output_file = 'subsampling_analysis_benchmark'

    with open(f"results/{output_file}.json", 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    create_line_graph(benchmark_data, "Time to Run Query", "Degree of Subsampling",
                      "Seconds", f"figures/{output_file}.png", yscale='linear')

    if args.cleanup:
        cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
