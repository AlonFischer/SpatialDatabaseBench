import matplotlib.pyplot as plt
import numpy as np
from functools import reduce
import logging

color1 = "xkcd:royal blue"
color2 = "xkcd:crimson"


def create_bar_chart(data, title, y_axis_label, filename):
    """ data is a dictionary of category (string) to dictionaries. Each inner dictionary is a dictionary of bar label (string) to bar height (number)
    """
    logger = logging.getLogger(__name__)
    fig, ax = plt.subplots(figsize=(10, 5))

    num_series = len(data)
    width = 0.3

    labels = reduce(np.union1d, [list(data[entry].keys()) for entry in data])

    num_data_points = len(labels)
    indexes = np.arange(num_data_points) * (num_series+1) * width

    for idx, series in enumerate(data):
        bar_heights = [data[series].get(label, 0) for label in labels]
        ax.bar(indexes + width * idx, bar_heights, width, label=series)

    # return

    # ax.bar(np.arange(n), data1, width, label="Postgres", color=color1)
    # ax.bar(np.arange(n) + width, data2, width, label="MySQL", color=color2)

    ax.set_xticks(indexes + width/2 * (num_series-1))
    ax.set_xticklabels(labels, rotation=45, ha='right')
    plt.ylabel(y_axis_label)

    ax.legend(loc='best')
    ax.set_title(title)

    fig.savefig(filename, bbox_inches='tight')
