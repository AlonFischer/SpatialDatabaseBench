import matplotlib.pyplot as plt
from matplotlib import rc, rcParams
from matplotlib.ticker import FormatStrFormatter
import numpy as np
from functools import reduce
import logging
from fractions import Fraction
import json
import argparse

logger = logging.getLogger(__name__)


def create_line_graph(data, title, x_axis_label, y_axis_label, filename, yscale='linear'):
    """ data is a dictionary of category (string) to dictionaries. Each inner dictionary is a dictionary of bar label (string) to bar height (number)
    """
    fig, ax = plt.subplots(figsize=(10, 5))

    rcParams['mathtext.default'] = 'regular'

    # num_series = len(data)
    # width = 0.3

    # x_ticks = reduce(np.union1d, [list(data[entry].keys()) for entry in data])
    # x_tick_labels = [r'$\frac{%s}{%s}$' % (
    #     Fraction(t).numerator, Fraction(t).denominator) if Fraction(t) >= Fraction(1/32) else '' for t in x_ticks]
    # x_tick_labels[-1] = 1

    # num_data_points = len(labels)
    # indexes = np.arange(num_data_points) * (num_series+1) * width

    for idx, series in enumerate(data):
        # bar_heights = [data[series].get(label, 0) for label in labels]
        x_vals = np.array(list(data[series].keys()))
        y_vals = np.array([data[series][x_val] for x_val in x_vals])
        ax.plot(x_vals, y_vals, label=series, zorder=2, marker='o',
                linewidth=2, markersize=6)

    # Axis labels
    # ax.set_xticks(x_ticks)
    # ax.set_xticklabels(x_tick_labels, fontdict={
    #                    'fontsize': rcParams['axes.titlesize']})
    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    plt.yscale('log')
    plt.xscale('log')
    ax.xaxis.set_major_formatter(FormatStrFormatter('%.3g'))
    ax.xaxis.set_minor_formatter(FormatStrFormatter('%.3g'))
    ax.yaxis.set_major_formatter(FormatStrFormatter('%.3g'))

    # horizontal gridlines
    ax.grid(axis='y', linestyle=':', zorder=1)

    # Misc properties
    ax.legend(bbox_to_anchor=(1, 1), loc="upper left")
    ax.set_title(title)

    fig.savefig(filename, bbox_inches='tight', dpi=300)


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('mode', metavar='M', type=str,
                        choices=['join', 'analysis'],
                        help='Constrains which benchmarks are run')
    args = parser.parse_args()

    output_file = f"subsampling_{args.mode}_benchmark"
    benchmark_data = {}
    with open(f"results/{output_file}.json", 'r') as file:
        benchmark_data = json.loads(file.read(),
                                    object_hook=lambda d: {float(k) if isfloat(k) else k: v for k, v in d.items()})

    logger.info(benchmark_data)

    create_line_graph(benchmark_data, "Time to Run Query With Different Dataset Sizes", "Size of Dataset Relative to Original",
                      "Seconds", f"figures/{output_file}.png", yscale='linear')
