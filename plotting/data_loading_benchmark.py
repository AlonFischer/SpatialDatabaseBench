import argparse
import json
import logging
from bar_chart import create_bar_chart

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    output_file = "data_loading_benchmark"
    data_file = "data_loading_benchmark.json"

    benchmark_data = {}
    with open(f"results/{data_file}", 'r') as file:
        benchmark_data.update(json.loads(file.read()))

    logger.info(benchmark_data)

    create_bar_chart(benchmark_data, "Time to Load Dataset",
                     "Seconds", f"figures/{output_file}.png", yscale='log')
