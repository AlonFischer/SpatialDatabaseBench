import numpy as np
from bar_chart import create_bar_chart

def main():
    labels = ["test1", "test2", "test3", "test4", "test5"]
    data1 = np.array([10, 11, 12, 13, 14])
    data2 = data1 * 2

    create_bar_chart(labels, data1, data2, "Postgres", "MySQL", "Test Plot", "figures/test_plot.png")

    

if __name__ == "__main__":
    main()