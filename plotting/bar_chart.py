import matplotlib.pyplot as plt
from matplotlib.ticker import FormatStrFormatter
import numpy as np
from functools import reduce
import logging
import json
import argparse


logger = logging.getLogger(__name__)

# from http://composition.al/blog/2015/11/29/a-better-way-to-add-labels-to-bar-charts-with-matplotlib/


def autolabel(rects, ax):
    # Get y-axis height to calculate label position from.
    (y_bottom, y_top) = ax.get_ylim()
    y_height = y_top - y_bottom

    for rect in rects:
        height = rect.get_height()
        label_position = height + height * 0.05  # + (y_height * 0.01)

        ax.text(rect.get_x() + rect.get_width()/2., label_position,
                '{:.4g}'.format(float(height)),
                ha='center', va='bottom', rotation=90)


def create_bar_chart(data, title, y_axis_label, filename, yscale='linear'):
    """ data is a dictionary of category (string) to dictionaries. Each inner dictionary is a dictionary of bar label (string) to bar height (number)
    """
    fig, ax = plt.subplots(figsize=(10, 5))

    num_series = len(data)
    width = 0.3

    labels = reduce(np.union1d, [list(data[entry].keys()) for entry in data])

    num_data_points = len(labels)
    indexes = np.arange(num_data_points) * (num_series+1) * width

    for idx, series in enumerate(data):
        bar_heights = [data[series].get(label, 0) for label in labels]
        rects = ax.bar(indexes + width * idx, bar_heights,
                       width, label=series, zorder=2)
        autolabel(rects, ax)

    # axis labels
    ax.set_xticks(indexes + width/2 * (num_series-1))
    ax.set_xticklabels(labels, rotation=-45, ha='left')
    plt.ylabel(y_axis_label)
    plt.yscale(yscale)
    ax.yaxis.set_major_formatter(FormatStrFormatter('%.4g'))

    # horizontal gridlines
    ax.grid(axis='y', linestyle=':', zorder=1)

    # misc properties
    ax.legend(bbox_to_anchor=(1, 1), loc="upper left")
    ax.set_title(title)
    ax.margins(y=0.15)

    fig.savefig(filename, bbox_inches='tight', dpi=300)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('mode', metavar='M', type=str,
                        choices=['join', 'analysis'],
                        help='Constrains which benchmarks are run')
    args = parser.parse_args()

    output_file = f"{args.mode}_benchmark"
    benchmark_data = {}
    with open(f"results/{output_file}.json", 'r') as file:
        benchmark_data = json.loads(file.read())

    logger.info(benchmark_data)

    create_bar_chart(benchmark_data, "Time to Run Query",
                     "Seconds", f"figures/{output_file}.png", yscale='log')
