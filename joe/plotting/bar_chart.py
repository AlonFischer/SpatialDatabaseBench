import matplotlib.pyplot as plt
import numpy as np

color1 = "xkcd:royal blue"
color2 = "xkcd:crimson"

def create_bar_chart(groups, data1, data2, data1_label, data2_label, title, filename):
    fig, ax = plt.subplots(figsize=(10,5))

    n = len(groups)
    width = 0.3

    ax.bar(np.arange(n), data1, width, label="Postgres", color=color1)
    ax.bar(np.arange(n) + width, data2, width, label="MySQL", color=color2)

    ax.set_xticks(np.arange(n) + width / 2)
    ax.set_xticklabels(groups, rotation=45, ha='right')
    plt.ylabel('Time (ms)')

    ax.legend(loc='best')
    ax.set_title(title)

    fig.savefig(filename, bbox_inches='tight')