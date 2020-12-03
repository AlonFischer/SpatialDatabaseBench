import argparse
import json
import logging
from bar_chart import create_bar_chart

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    output_file = "data_insertion_benchmark"
    data_files = [f"data_insertion_benchmark_pg_index_GIST.json",
                  f"data_insertion_benchmark_no_mysql_index_pg_index_NONE.json"]

    benchmark_data = {}
    for data_file in data_files:
        with open(f"results/{data_file}", 'r') as file:
            benchmark_data.update(json.loads(file.read()))

    logger.info(benchmark_data)

    create_bar_chart(benchmark_data, "Time to Insert 1000 Rows",
                     "Seconds", f"figures/{output_file}.png", yscale='log')
