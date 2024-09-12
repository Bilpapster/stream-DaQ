import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation


def get_subplot(dataframe: pd.DataFrame, y_dataframe_column: str, legend_label: str, color: str):
    plt.cla()
    plt.xticks(rotation=75)
    plt.grid(visible=True, alpha=0.3)
    plt.plot(dataframe['timestamp'], dataframe[y_dataframe_column], label=legend_label, color=color, linestyle='--', linewidth=0.5)
    plt.fill_between(dataframe['timestamp'], dataframe[y_dataframe_column], color=color, alpha=0.2, interpolate=True)
    plt.legend(loc='lower left')


def update_dq_dashboard(dummy):
    data = pd.read_csv('data.csv', names=[
        'timestamp',
        'window_id',
        'max',
        'min',
        'mean',
        'count',
        'distinct'
    ], parse_dates=['timestamp'])

    plt.subplot(231)
    get_subplot(data, 'max', 'max', 'blue')

    plt.subplot(232)
    get_subplot(data, 'min', 'min', 'green')

    plt.subplot(233)
    get_subplot(data, 'mean', 'mean', 'yellowgreen')

    plt.subplot(234)
    get_subplot(data, 'count', 'count', 'magenta')

    plt.subplot(235)
    get_subplot(data, 'distinct', 'distinct', 'orange')


plt.style.use('fivethirtyeight')
animation = FuncAnimation(plt.gcf(), update_dq_dashboard, interval=1000, cache_frame_data=False)
plt.show()
