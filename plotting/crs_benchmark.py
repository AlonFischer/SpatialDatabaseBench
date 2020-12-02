import argparse
import json
import logging
from bar_chart import create_bar_chart

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('mode', metavar='M', type=str,
                        choices=['join', 'analysis'],
                        help='Constrains which benchmarks are run')
    args = parser.parse_args()

    output_file = f"{args.mode}_crs_benchmark"
    data_files = [f"{args.mode}_benchmark_pg_index_GIST.json",
                  f"{args.mode}_benchmark_pg_index_GIST_gcs.json"]

    benchmark_data = {}
    for data_file in data_files:
        with open(f"results/{data_file}", 'r') as file:
            benchmark_data.update(json.loads(file.read()))

    logger.info(benchmark_data)

    create_bar_chart(benchmark_data, "Time to Run Query With PCS and GCS",
                     "Seconds", f"figures/{output_file}.png", yscale='log', fig_size=(15, 5))
