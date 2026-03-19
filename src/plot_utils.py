import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# 分布をプロットする関数
def plot_distribution(
    df,
    column_name={
        'vin': 'MASKED_VIN',
    },
    target_col='MAX_SPEED',
    distribution_type="frequency",
    plt_params={
        'figsize': (10, 5),
        'bin_width': 10,
        'title': 'Distribution of Max Speed',
        'xlabel': 'Max Speed (km/h)',
        'color': 'C0',
        'xlim': (None, None)
                }):
    if distribution_type=="frequency":
        data = df[target_col]
        ylabel = "Frequency"
    elif distribution_type=="number_of_vehicles":         
        data = (
            df.groupby(column_name['vin'], as_index=False)[target_col]
            .max()
        )[target_col]
        ylabel = "Number of vehicles"
    bins = range(
        int(data.min() // plt_params['bin_width'] * plt_params['bin_width']),
        int(data.max() // plt_params['bin_width'] * plt_params['bin_width'] + plt_params['bin_width']),
        plt_params['bin_width']
        )
    fig, ax = plt.subplots(figsize=plt_params['figsize'])
    ax.hist(data, bins=bins, color=plt_params['color'], edgecolor='k')
    ax.set_xlabel(plt_params['xlabel'])
    ax.set_ylabel(ylabel)
    ax.set_xlim(plt_params['xlim'])
    fig.suptitle(plt_params['title'])
    plt.tight_layout()
    plt.show()


# 累積付き連続値分布をプロットする関数
def plot_distribution_with_cdf(
    df,
    column_name={
        "vin": "MASKED_VIN",
    },
    target_col='DRIVING_COUNT',
    plt_params={
    'figsize': (10, 5),
    'bin_width': 10,
    'bar_color': 'C0',
    'xlabel': 'Number of Circuit Driving',
    'bar_ylabel': 'Number of Vehicles (log scale)',
    'bar_yscale': 'linear',
    'cdf_ylabel': 'Cumulative Distribution (%)',
    'title': 'Distribution of Vehicle Circuit Driving Counts',
    'cdf_color': 'C1',
    'cdf_marker': '.',
    'xlim': (None, None)
    }
):
    data = (
            df.groupby(column_name['vin'], as_index=False)[target_col]
            .max()
        )[target_col]
    bins = range(
        int(data.min() // plt_params['bin_width'] * plt_params['bin_width']),
        int(data.max() // plt_params['bin_width'] * plt_params['bin_width'] + plt_params['bin_width']),
        plt_params['bin_width']
        )
    fig, ax1 = plt.subplots(figsize=plt_params['figsize'])
    n, bins, _ = ax1.hist(data, bins=bins, color=plt_params['bar_color'], edgecolor='k')
    ax1.set_xlabel(plt_params['xlabel'])
    ax1.set_ylabel(plt_params['bar_ylabel'], color=plt_params['bar_color'])
    ax1.set_title(plt_params['title'])
    ax1.set_yscale(plt_params['bar_yscale'])
    ax1.tick_params(axis='y', labelcolor=plt_params['bar_color'])
    ax1.set_xlim(plt_params['xlim'])
    ax2 = ax1.twinx()
    y2 = np.add.accumulate(n) / n.sum()
    x2 = np.convolve(bins, np.ones(2) / 2, mode="same")[1:]
    ax2.plot(x2, y2*100, color=plt_params['cdf_color'], marker=plt_params['cdf_marker'])
    ax2.set_ylabel(plt_params['cdf_ylabel'], color=plt_params['cdf_color'])
    ax2.tick_params(axis='y', labelcolor=plt_params['cdf_color'])
    ax2.set_xlim(plt_params['xlim'])
    plt.grid(axis="y", linestyle="--")
    plt.tight_layout()
    plt.show()


# 累積付きの台数分布をプロットする関数
def plot_count_distribution_with_cdf(
    df,
    column_name={
        "vin": "MASKED_VIN",
        "model_type": "DISPATCH_MODEL_TYPE",
    },
    count_col='DRIVING_COUNT',
    plt_params={
    'figsize': (10, 5),
    'bar_color': 'C0',
    'xlabel': 'Number of Circuit Driving',
    'bar_ylabel': 'Number of Vehicles (log scale)',
    'bar_yscale': 'log',
    'cdf_ylabel': 'Cumulative Distribution (%)',
    'title': 'Distribution of Vehicle Circuit Driving Counts',
    'cdf_color': 'C1',
    'cdf_marker': '.',
    }
):
    df = df.fillna(0).astype(int, errors='ignore')
    out = pd.crosstab(
        index=[df[column_name['vin']]],
        columns=df[count_col],
    )
    distribution = df[count_col].value_counts().sort_index()
    cdf = distribution.cumsum() / distribution.sum()
    fig, ax1 = plt.subplots(figsize=plt_params['figsize'])
    bars = ax1.bar(distribution.index, distribution.values,  color=plt_params['bar_color'])
    ax1.bar_label(bars, labels=distribution.values)
    ax1.set_xlabel(plt_params['xlabel'])
    ax1.set_ylabel(plt_params['bar_ylabel'], color=plt_params['bar_color'])
    ax1.set_title(plt_params['title'])
    ax1.set_yscale(plt_params['bar_yscale'])
    ax1.tick_params(axis='y', labelcolor=plt_params['bar_color'])
    ax2 = ax1.twinx()
    ax2.plot(cdf.index, cdf.values*100, color=plt_params['cdf_color'], marker=plt_params['cdf_marker'])
    ax2.set_ylabel(plt_params['cdf_ylabel'], color=plt_params['cdf_color'])
    ax2.tick_params(axis='y', labelcolor=plt_params['cdf_color'])
    plt.xticks(distribution.index)
    plt.tight_layout()
    plt.grid(axis="y", linestyle="--")
    plt.show()


# 分布比較をプロットする関数
def plt_distribution_comparison(
    df1,
    df2,
    target_col='',
    plt_params={
        'figsize': (10, 5),
        'bin_width': 10,
        'color1': 'C0',
        'color2': 'C1',
        'label1': 'No Circuit Driving',
        'label2': 'Circuit Driving',
        'xlabel': 'Driving Distance (km)',
        'ylabel1': 'Number of Vehicles',
        'ylabel2': 'Number of Vehicles',
        'title': 'Distribution of Driving Speed in Circuit',
        'alpha': 0.5,
        'legend_loc': 'upper right'
    }
):
    fig, ax1 = plt.subplots(figsize=plt_params['figsize'])
    bins = np.arange(np.floor(np.nanmin(df1[target_col]) // plt_params['bin_width'] * plt_params['bin_width']),
                 np.ceil(np.nanmax(df1[target_col]) // plt_params['bin_width'] * plt_params['bin_width'] + plt_params['bin_width']),
                 plt_params['bin_width'])
    n, bins, patches = ax1.hist(df1[target_col], bins=bins, color=plt_params['color1'],
                                alpha=plt_params['alpha'], edgecolor=plt_params['color1'],
                                label=plt_params['label1'])
    ax1.set_xlabel(plt_params['xlabel'])
    ax1.set_ylabel(plt_params['ylabel1'], color=plt_params['color1'])
    ax1.tick_params(axis='y', labelcolor=plt_params['color1'])
    ax1.set_title(plt_params['title'])
    ax2 = ax1.twinx()
    bins = np.arange(np.floor(np.nanmin(df2[target_col]) // plt_params['bin_width'] * plt_params['bin_width']),
                 np.ceil(np.nanmax(df2[target_col]) // plt_params['bin_width'] * plt_params['bin_width'] + plt_params['bin_width']),
                 plt_params['bin_width'])
    n, bins, patches = ax2.hist(df2[target_col], bins=bins, color=plt_params['color2'],
         alpha=plt_params['alpha'], edgecolor=plt_params['color2'],
                                label=plt_params['label2'])
    ax2.set_ylabel(plt_params['ylabel2'], color=plt_params['color2'])
    ax2.tick_params(axis='y', labelcolor=plt_params['color2'])
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc=plt_params['legend_loc'], frameon=False)
    plt.tight_layout()
    plt.show()
    return


