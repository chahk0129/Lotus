import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid", {'axes.grid': True})
import pandas as pd

# Define colors and styles for different configurations
colors = {
    'Sherman/Deft NO_WAIT': 'crimson',
    'Sherman/Deft WAIT_DIE': 'darkorange',
    'Lotus NO_WAIT': 'midnightblue',
    'Lotus NO_WAIT + Batching': 'midnightblue',
    'Lotus WAIT_DIE': 'darkgoldenrod',
    'Lotus WAIT_DIE + Batching': 'darkgoldenrod',
    'Lotus WOUND_WAIT': 'darkolivegreen',
    'Lotus WOUND_WAIT + Batching': 'darkolivegreen'
}

linestyles = {
    'Sherman/Deft NO_WAIT': '-',
    'Sherman/Deft WAIT_DIE': '-',
    'Lotus NO_WAIT': '-',
    'Lotus NO_WAIT + Batching': ':',
    'Lotus WAIT_DIE': '-',
    'Lotus WAIT_DIE + Batching': ':',
    'Lotus WOUND_WAIT': '-',
    'Lotus WOUND_WAIT + Batching': ':'
}

labels = {
    'Sherman/Deft NO_WAIT': 'Sherman/Deft NO_WAIT',
    'Sherman/Deft WAIT_DIE': 'Sherman/Deft WAIT_DIE',
    'Lotus NO_WAIT': 'Lotus NO_WAIT',
    'Lotus NO_WAIT + Batching': 'Lotus NO_WAIT (Batch=1)',
    'Lotus WAIT_DIE': 'Lotus WAIT_DIE',
    'Lotus WAIT_DIE + Batching': 'Lotus WAIT_DIE (Batch=1)',
    'Lotus WOUND_WAIT': 'Lotus WOUND_WAIT',
    'Lotus WOUND_WAIT + Batching': 'Lotus WOUND_WAIT (Batch=1)'
}

markers = {
    'Sherman/Deft NO_WAIT': '^',
    'Sherman/Deft WAIT_DIE': 'P',
    'Lotus NO_WAIT': 'v',
    'Lotus WAIT_DIE': 'X',
    'Lotus WOUND_WAIT': 'o',
    'Lotus NO_WAIT + Batching': 'v',
    'Lotus WAIT_DIE + Batching': 'X',
    'Lotus WOUND_WAIT + Batching': 'o'
}

# Legend order: Lotus on the top row, Sherman/Deft below.
# matplotlib fills legend entries column by column, so with ncol=3 the list is
# interleaved (col 1: Lotus NO_WAIT, Sherman NO_WAIT; col 2: ...; col 3: Lotus WOUND_WAIT).
order = [labels[k] for k in
         ['Lotus NO_WAIT', 'Sherman/Deft NO_WAIT',
          'Lotus WAIT_DIE', 'Sherman/Deft WAIT_DIE',
          'Lotus WOUND_WAIT']]


def read_data(file_path):
    # Rows have a variable number of trailing stat columns (the header only covers the
    # longest row), so parse with csv and keep the columns named in the header.
    with open(file_path) as f:
        rows = list(csv.reader(f))
    header, body = rows[0], rows[1:]
    records = [dict(zip(header, r)) for r in body if r]
    df = pd.DataFrame(records)
    for col in ['BATCH', 'NUM_CLIENT_THREADS', 'NUM_WH', 'Throughput']:
        df[col] = pd.to_numeric(df[col])
    return df


def tpcc_scalability(df_list):
    df = pd.concat(df_list, ignore_index=True)
    df['Throughput'] = df['Throughput'] / 1000

    # NUM_WH is per server; 8 servers in total
    settings = {
        '32': {'NUM_WH': 4, 'title': '(a) 32 Warehouses'},
        '512': {'NUM_WH': 64, 'title': '(b) 512 Warehouses'},
    }

    fig, axes = plt.subplots(1, 2, figsize=(9.2, 4.2))

    marker_size = 12
    for idx, (key, setting) in enumerate(settings.items()):
        ax = axes[idx]
        data = df[df['NUM_WH'] == setting['NUM_WH']]
        if data.empty:
            print(f"No data found for {setting['title']}")
            continue

        series = [('ONE_SIDED', cc, 0, f'Sherman/Deft {cc}') for cc in ['NO_WAIT', 'WAIT_DIE']] + \
                 [('TWO_SIDED', cc, 1, f'Lotus {cc}') for cc in ['NO_WAIT', 'WAIT_DIE', 'WOUND_WAIT']]
        for transport, cc_alg, batch, config_key in series:
            d = data[(data['TRANSPORT'] == transport) &
                     (data['CC_ALG'] == cc_alg) &
                     (data['BATCH'] == batch)]
            if d.empty:
                continue
            d = d.sort_values('NUM_CLIENT_THREADS')
            ax.plot(d['NUM_CLIENT_THREADS'], d['Throughput'],
                    color=colors[config_key],
                    linestyle=linestyles[config_key],
                    linewidth=2,
                    marker=markers[config_key],
                    markersize=marker_size,
                    markerfacecolor=colors[config_key],
                    markeredgewidth=1,
                    label=labels[config_key])

        ax.set_xlabel('Number of Threads\nper Compute Server', fontsize=20)
        if idx == 0:
            ax.set_ylabel('Throughput\n(Ktxns/sec)', fontsize=20)
        ax.set_title(setting['title'], fontsize=20)

        unique_threads = sorted(data['NUM_CLIENT_THREADS'].unique())
        ax.set_xticks(unique_threads)
        ax.set_xticklabels([str(x) for x in unique_threads])

        ax.grid(True)
        ax.tick_params(labelsize=18)
        max_throughput = data['Throughput'].max()
        ax.set_ylim(0, max_throughput * 1.1)

    all_handles, all_labels = [], []
    for ax in axes:
        h, l = ax.get_legend_handles_labels()
        for handle, label in zip(h, l):
            if label not in all_labels:
                all_handles.append(handle)
                all_labels.append(label)

    ordered_handles, ordered_labels = [], []
    for desired_label in order:
        if desired_label in all_labels:
            i = all_labels.index(desired_label)
            ordered_handles.append(all_handles[i])
            ordered_labels.append(all_labels[i])
    for handle, label in zip(all_handles, all_labels):
        if label not in ordered_labels:
            ordered_handles.append(handle)
            ordered_labels.append(label)

    fig.legend(ordered_handles, ordered_labels,
               loc='upper center',
               bbox_to_anchor=(0.52, 1.0),
               ncol=3,
               fontsize=18,
               frameon=True,
               fancybox=True,
               shadow=False,
               handlelength=1.5,
               handletextpad=0.25,
               columnspacing=1.5,
               labelspacing=0.7)

    plt.tight_layout()
    plt.subplots_adjust(top=0.67)

    os.makedirs("figures", exist_ok=True)
    plt.savefig("figures/tpcc_general_throughput.pdf", bbox_inches='tight')
    plt.savefig("figures/tpcc_general_throughput.png", bbox_inches='tight', dpi=150)
    plt.show()


data_path_tpcc = "outputs/tpcc_general/throughput"
tput_files = ["4", "64"]  # warehouses per server -> 32 and 512 in total

data_tpcc_tput = [read_data(f"{data_path_tpcc}/{f}.csv") for f in tput_files]
tpcc_scalability(data_tpcc_tput)
