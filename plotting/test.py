import numpy as np
from bar_chart import create_bar_chart
import logging


def main():
    logging.basicConfig(level=logging.INFO)
    labels = ["test1", "test2", "test3", "test4", "test5"]
    data1 = np.array([10, 11, 12, 13, 14])
    data2 = data1 * 2

    chart_data = {
        "MySQL": {
            "test1": 0.10,
            "test2": 0.20
        },
        "Postgres": {
            "test1": 0.20,
            "test2": 0.40
        },
    }

    create_bar_chart(chart_data, "Test Plot",
                     "Time (sec)", "figures/test_plot.png")


if __name__ == "__main__":
    main()
