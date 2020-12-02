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

    output_file = f"{args.mode}_index_benchmark"
    data_files = []
    if args.mode == "analysis":
        data_files = [f"{args.mode}_benchmark_pg_index_GIST.json",
                      f"{args.mode}_benchmark_no_mysql_index_pg_index_NONE.json",
                      f"{args.mode}_benchmark_pg_index_BRIN.json",
                      f"{args.mode}_benchmark_pg_index_SPGIST.json"]
    elif args.mode == "join":
        data_files = [f"{args.mode}_benchmark_pg_index_GIST.json",
                      f"{args.mode}_benchmark_pg_index_SPGIST.json"]

    benchmark_data = {}
    for data_file in data_files:
        with open(f"results/{data_file}", 'r') as file:
            benchmark_data.update(json.loads(file.read()))

    logger.info(benchmark_data)

    if args.mode == "analysis":
        benchmark_data_keys = [
            "MySQL", "MySQL (No Index)", "Postgis (GIST Index)", "Postgis (No Index)"]
        benchmark_subset = dict(
            (k, benchmark_data[k]) for k in benchmark_data_keys)
        create_bar_chart(benchmark_subset, "Time to Run Query With and Without Spatial Index",
                         "Seconds", f"figures/{output_file}_pg_mysql.png", yscale='log', fig_size=(15, 5))
        benchmark_data_keys = [
            "Postgis (GIST Index)", "Postgis (SPGIST Index)", "Postgis (BRIN Index)", "Postgis (No Index)"]
        benchmark_subset = dict(
            (k, benchmark_data[k]) for k in benchmark_data_keys)
        create_bar_chart(benchmark_subset, "Time to Run Query With and Without Spatial Index",
                         "Seconds", f"figures/{output_file}_pg_only.png", yscale='log', fig_size=(15, 5))
    elif args.mode == "join":
        create_bar_chart(benchmark_data, "Time to Run Query With and Without Spatial Index",
                         "Seconds", f"figures/{output_file}.png", yscale='log', fig_size=(10, 5))
