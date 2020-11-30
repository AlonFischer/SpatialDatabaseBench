import logging
import time
import json
import argparse
from benchmark import mysql_benchmarks
from plotting.bar_chart import create_bar_chart
from util.benchmark_helpers import init, cleanup

"""
Benchmark for measuring dataset sizes
"""

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    init(create_spatial_index=True)

    benchmarks = [
        ("MySQL", "Airspaces", mysql_benchmarks.AirspacesSize()),
        ("MySQL", "Airports", mysql_benchmarks.AirportsSize()),
        ("MySQL", "Routes", mysql_benchmarks.RoutesSize()),
    ]

    benchmark_data = dict([(benchmark[0], {}) for benchmark in benchmarks])
    for idx, bnchmrk in enumerate(benchmarks):
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        storage_space = bnchmrk[2].get_results()[0][0]
        logger.info(f"Storage Space: {storage_space} megabytes")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = storage_space

    cleanup()
    init(create_spatial_index=False)

    benchmarks = [
        ("MySQL (No Index)", "Airspaces", mysql_benchmarks.AirspacesSize()),
        ("MySQL (No Index)", "Airports", mysql_benchmarks.AirportsSize()),
        ("MySQL (No Index)", "Routes", mysql_benchmarks.RoutesSize()),
    ]

    benchmark_data.update(dict([(benchmark[0], {})
                                for benchmark in benchmarks]))
    for idx, bnchmrk in enumerate(benchmarks):
        logger.info(f"Starting benchmark {idx+1}")
        bnchmrk[2].run()
        storage_space = bnchmrk[2].get_results()[0][0]
        logger.info(f"Storage Space: {storage_space} megabytes")
        benchmark_data[bnchmrk[0]][bnchmrk[1]] = storage_space

    # Save raw benchmark data to file
    output_file = "storage_size_benchmark"
    with open(f"results/{output_file}.json", 'w') as file:
        file.write(json.dumps(benchmark_data, indent=4))

    create_bar_chart(benchmark_data, "Size of Dataset",
                     "Megabytes", f"figures/{output_file}.png", yscale='log')

    cleanup()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    logger.info(f"Total benchmark time: {(end-start)/60} minutes")
