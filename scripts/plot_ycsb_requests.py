import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid", {'axes.grid': True})
import matplotlib.ticker as ticker
import numpy as np
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
    for col in ['BATCH', 'NUM_CLIENT_THREADS', 'Throughput']:
        df[col] = pd.to_numeric(df[col])
    return df



def ycsb_requests(df_list):
    df = pd.concat(df_list, ignore_index=True)
    # Normalize to Lotus WOUND_WAIT (two-sided, BATCH=1, stored procedure) at the same
    # workload and transaction length
    lotus = (df['TRANSPORT'] == 'TWO_SIDED') & (df['BATCH'] == 1) & (df['TXN_TYPE'] == 'STORED_PROCEDURE')
    base_df = df[lotus & (df['CC_ALG'] == 'WOUND_WAIT')][
        ['READ_PERC', 'UPDATE_PERC', 'REQ_PER_QUERY', 'Throughput']]
    base_df = base_df.rename(columns={'Throughput': 'Base_Throughput'})
    df = df.merge(base_df, on=['READ_PERC', 'UPDATE_PERC', 'REQ_PER_QUERY'], how='left')
    df['Relative_Throughput'] = df['Throughput'] / df['Base_Throughput']
    workloads = {
        'A': {'READ_PERC': 0.5, 'UPDATE_PERC': 0.5, 'title': '(a) Workload A'},
        'B': {'READ_PERC': 0.95, 'UPDATE_PERC': 0.05, 'title': '(b) Workload B'},
        'C': {'READ_PERC': 1.0, 'UPDATE_PERC': 0.0, 'title': '(c) Workload C'},
    }

    fig, axes = plt.subplots(1, 3, figsize=(12.8, 4.8))
    marker_size = 12
    series = [('ONE_SIDED', cc, f'Sherman/Deft {cc}') for cc in ['NO_WAIT', 'WAIT_DIE']] + \
             [('TWO_SIDED', cc, f'Lotus {cc}') for cc in ['NO_WAIT', 'WAIT_DIE', 'WOUND_WAIT']]

    # Shared y range so the three panels are directly comparable
    y_top = df['Relative_Throughput'].max() * 1.1

    for idx, (_, cfg) in enumerate(workloads.items()):
        ax = axes[idx]
        ax.set_ylim(0, y_top)
        data = df[(df['READ_PERC'] == cfg['READ_PERC']) & (df['UPDATE_PERC'] == cfg['UPDATE_PERC'])]
        if data.empty:
            print(f"No data found for {cfg['title']}")
            continue
        x_vals = sorted(data['REQ_PER_QUERY'].unique())
        x_map = {v: i for i, v in enumerate(x_vals)}

        plotted = []
        for transport, cc_alg, key in series:
            d = data[(data['TRANSPORT'] == transport) & (data['CC_ALG'] == cc_alg)]
            if transport == 'TWO_SIDED':
                d = d[(d['BATCH'] == 1) & (d['TXN_TYPE'] == 'STORED_PROCEDURE')]
            if d.empty:
                continue
            d = d.sort_values('REQ_PER_QUERY')
            plotted.append(d)
            ax.plot(d['REQ_PER_QUERY'].map(x_map), d['Relative_Throughput'],
                    color=colors[key], linestyle=linestyles[key], linewidth=2,
                    marker=markers[key], markersize=marker_size,
                    markerfacecolor=colors[key], markeredgewidth=1,
                    label=labels[key])

        ax.set_xlabel('Number of Requests\nper Transaction', fontsize=20)
        if idx == 0:
            ax.set_ylabel('Normalized Throughput', fontsize=20)
        ax.set_title(cfg['title'], fontsize=20)
        ax.axhline(1.0, color='gray', linestyle='--', linewidth=1.5, zorder=0)
        ax.set_xticks(range(len(x_vals)))
        ax.set_xticklabels([str(x) for x in x_vals])
        ax.grid(True)
        ax.tick_params(labelsize=18)

    all_handles, all_labels = [], []
    for ax in axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in all_labels:
                all_handles.append(h)
                all_labels.append(l)
    ordered = [(all_handles[all_labels.index(l)], l) for l in order if l in all_labels]
    ordered += [(h, l) for h, l in zip(all_handles, all_labels) if l not in order]
    fig.legend([h for h, _ in ordered], [l for _, l in ordered],
               loc='upper center', bbox_to_anchor=(0.52, 1.0), ncol=3, fontsize=18,
               frameon=True, fancybox=True, shadow=False, handlelength=1.5,
               handletextpad=0.25, columnspacing=1.5, labelspacing=0.7)

    plt.tight_layout()
    plt.subplots_adjust(top=0.70)
    os.makedirs("figures", exist_ok=True)
    plt.savefig("figures/ycsb_request_throughput.pdf", bbox_inches='tight')
    plt.savefig("figures/ycsb_request_throughput.png", bbox_inches='tight', dpi=150)
    plt.show()


def ycsb_requests_speedup(df_list, y_cap=8.0):
    """Throughput normalized to Sherman/Deft WAIT_DIE on a linear y-axis capped at y_cap.
    Points above the cap are drawn at the cap with a tilde break mark and their value."""
    df = pd.concat(df_list, ignore_index=True)
    workloads = {
        'A': {'READ_PERC': 0.5, 'UPDATE_PERC': 0.5, 'title': '(a) Workload A'},
        'B': {'READ_PERC': 0.95, 'UPDATE_PERC': 0.05, 'title': '(b) Workload B'},
        'C': {'READ_PERC': 1.0, 'UPDATE_PERC': 0.0, 'title': '(c) Workload C'},
    }
    keys = ['READ_PERC', 'UPDATE_PERC', 'REQ_PER_QUERY']
    base = (df[(df['TRANSPORT'] == 'ONE_SIDED') & (df['CC_ALG'] == 'WAIT_DIE')][keys + ['Throughput']]
            .rename(columns={'Throughput': 'Base_Throughput'}))
    df = df[(df['TRANSPORT'] == 'ONE_SIDED') |
            ((df['BATCH'] == 1) & (df['TXN_TYPE'] == 'STORED_PROCEDURE'))]
    df = df.merge(base, on=keys, how='left')
    df['Speedup'] = df['Throughput'] / df['Base_Throughput']

    series = [('TWO_SIDED', cc, f'Lotus {cc}') for cc in ['NO_WAIT', 'WAIT_DIE', 'WOUND_WAIT']] + \
             [('ONE_SIDED', cc, f'Sherman/Deft {cc}') for cc in ['NO_WAIT', 'WAIT_DIE']]

    fig, axes = plt.subplots(1, 3, figsize=(14.3, 5.0))
    breaks = []

    for idx, (_, cfg) in enumerate(workloads.items()):
        ax = axes[idx]
        data = df[(df['READ_PERC'] == cfg['READ_PERC']) & (df['UPDATE_PERC'] == cfg['UPDATE_PERC'])]
        if data.empty:
            print(f"No data found for {cfg['title']}")
            continue
        x_vals = sorted(data['REQ_PER_QUERY'].unique())
        xs = np.arange(len(x_vals))
        capped = data['Speedup'].max() > y_cap
        top = y_cap if capped else data['Speedup'].max() * 1.15

        # how many series are clipped at each x, so their markers can be spread apart
        n_clip = {}
        for transport, cc, key in series:
            d = data[(data['TRANSPORT'] == transport) & (data['CC_ALG'] == cc)] \
                .set_index('REQ_PER_QUERY').reindex(x_vals)
            for i in np.where(d['Speedup'].to_numpy() > top)[0]:
                n_clip[i] = n_clip.get(i, 0) + 1
        placed = {}
        for transport, cc, key in series:
            d = data[(data['TRANSPORT'] == transport) & (data['CC_ALG'] == cc)] \
                .set_index('REQ_PER_QUERY').reindex(x_vals)
            v = d['Speedup'].to_numpy()
            if np.all(np.isnan(v)):
                continue
            over = v > top
            v_plot = np.where(over, top, v)
            x_plot = xs.astype(float).copy()
            for i in np.where(over)[0]:
                n = placed.get(i, 0)
                placed[i] = n + 1
                x_plot[i] += (n - (n_clip[i] - 1) / 2) * 0.22
            ax.plot(x_plot, v_plot, color=colors[key], linestyle=linestyles[key], linewidth=2,
                    marker=markers[key], markersize=12, markerfacecolor=colors[key],
                    markeredgewidth=1, label=labels[key], clip_on=False, zorder=3)
            for i in np.where(over)[0]:
                # remember the segment leading to the clipped point; the break mark is
                # drawn after layout so it can be oriented in screen space
                if i > 0:
                    breaks.append((ax, x_plot[i - 1], v_plot[i - 1], x_plot[i], top, colors[key]))
                # actual value in the headroom band above the cap, stacked when several share an x
                txt = f'{v[i]:.0f}x' if v[i] >= 10 else f'{v[i]:.1f}x'
                ax.annotate(txt, (x_plot[i], top), textcoords='offset points',
                            xytext=(-10, 8 + 20 * (placed[i] - 1)), ha='right', va='bottom',
                            fontsize=14, fontweight='bold', color=colors[key],
                            annotation_clip=False)
        if n_clip:
            ax.set_xlim(-0.35, len(x_vals) - 1 + 0.45)

        ax.axhline(1.0, color='gray', linestyle='--', linewidth=1.5, zorder=0)
        if n_clip:
            ax.set_ylim(0, top * 1.22)
            ax.set_yticks([t for t in ax.get_yticks() if 0 <= t <= top])
            ax.axhline(top, color='black', linewidth=0.8, linestyle=':', zorder=1)
        else:
            ax.set_ylim(0, top)
        ax.set_xticks(xs)
        ax.set_xticklabels([str(x) for x in x_vals])
        ax.set_xlabel('Number of Requests\nper Transaction', fontsize=20)
        if idx == 0:
            ax.set_ylabel('Normalized Throughput', fontsize=20)
        ax.set_title(cfg['title'], fontsize=20)
        ax.grid(True)
        ax.tick_params(labelsize=18)

    h, l = axes[0].get_legend_handles_labels()
    pairs = dict(zip(l, h))
    ordered = [l for l in order if l in pairs]
    fig.legend([pairs[l] for l in ordered], ordered, loc='upper center', bbox_to_anchor=(0.52, 1.0),
               ncol=3, fontsize=18, frameon=True, fancybox=True, shadow=False, handlelength=1.5,
               handletextpad=0.25, columnspacing=1.5, labelspacing=0.7)
    plt.tight_layout()
    plt.subplots_adjust(top=0.70)

    # break marks: a short white gap in the segment with two parallel slashes across it
    for ax, x0, y0, x1, y1, color in breaks:
        to_disp, to_data = ax.transData, ax.transData.inverted()
        p0, p1 = to_disp.transform((x0, y0)), to_disp.transform((x1, y1))
        mid = (p0 + p1) / 2
        u = (p1 - p0) / np.linalg.norm(p1 - p0)          # along the segment
        n = np.array([-u[1], u[0]])                       # across the segment
        slash = n * np.cos(np.radians(25)) + u * np.sin(np.radians(25))
        gap, half = 5.0, 11.0                             # pixels at 100 dpi
        scale = fig.dpi / 100
        g0, g1 = to_data.transform([mid - u * gap * scale, mid + u * gap * scale])
        ax.plot([g0[0], g1[0]], [g0[1], g1[1]], color='white', linewidth=6,
                solid_capstyle='butt', zorder=4, clip_on=False)
        for c in (mid - u * gap * scale, mid + u * gap * scale):
            e0, e1 = to_data.transform([c - slash * half * scale, c + slash * half * scale])
            ax.plot([e0[0], e1[0]], [e0[1], e1[1]], color=color, linewidth=2.2,
                    solid_capstyle='round', zorder=5, clip_on=False)

    os.makedirs("figures", exist_ok=True)
    plt.savefig("figures/ycsb_request_speedup.pdf", bbox_inches='tight')
    plt.savefig("figures/ycsb_request_speedup.png", bbox_inches='tight', dpi=150)
    plt.show()


def ycsb_requests_reqtput(df_list):
    """Absolute request throughput (transactions/sec x requests per transaction), Mreqs/sec."""
    df = pd.concat(df_list, ignore_index=True)
    df = df[(df['TRANSPORT'] == 'ONE_SIDED') |
            ((df['BATCH'] == 1) & (df['TXN_TYPE'] == 'STORED_PROCEDURE'))].copy()
    df['Req_Throughput'] = df['Throughput'] * df['REQ_PER_QUERY'] / 1e6
    workloads = {
        'A': {'READ_PERC': 0.5, 'UPDATE_PERC': 0.5, 'title': '(a) Workload A'},
        'B': {'READ_PERC': 0.95, 'UPDATE_PERC': 0.05, 'title': '(b) Workload B'},
        'C': {'READ_PERC': 1.0, 'UPDATE_PERC': 0.0, 'title': '(c) Workload C'},
    }
    series = [('TWO_SIDED', cc, f'Lotus {cc}') for cc in ['NO_WAIT', 'WAIT_DIE', 'WOUND_WAIT']] + \
             [('ONE_SIDED', cc, f'Sherman/Deft {cc}') for cc in ['NO_WAIT', 'WAIT_DIE']]

    fig, axes = plt.subplots(1, 3, figsize=(14.3, 4.0))
    for idx, (_, cfg) in enumerate(workloads.items()):
        ax = axes[idx]
        data = df[(df['READ_PERC'] == cfg['READ_PERC']) & (df['UPDATE_PERC'] == cfg['UPDATE_PERC'])]
        if data.empty:
            print(f"No data found for {cfg['title']}")
            continue
        x_vals = sorted(data['REQ_PER_QUERY'].unique())
        xs = np.arange(len(x_vals))
        for transport, cc, key in series:
            d = data[(data['TRANSPORT'] == transport) & (data['CC_ALG'] == cc)] \
                .set_index('REQ_PER_QUERY').reindex(x_vals)
            ax.plot(xs, d['Req_Throughput'], color=colors[key], linestyle=linestyles[key],
                    linewidth=2, marker=markers[key], markersize=12,
                    markerfacecolor=colors[key], markeredgewidth=1, label=labels[key])
        ax.set_ylim(0, data['Req_Throughput'].max() * 1.1)
        ax.set_xticks(xs)
        ax.set_xticklabels([str(x) for x in x_vals])
        ax.set_xlabel('Number of Requests\nper Transaction', fontsize=20)
        if idx == 0:
            ax.set_ylabel('Throughput\n(Mreqs/sec)', fontsize=20)
        ax.set_title(cfg['title'], fontsize=20)
        ax.grid(True)
        ax.tick_params(labelsize=18)

    h, l = axes[0].get_legend_handles_labels()
    pairs = dict(zip(l, h))
    ordered = [l for l in order if l in pairs]
    fig.legend([pairs[l] for l in ordered], ordered, loc='upper center', bbox_to_anchor=(0.52, 1.0),
               ncol=3, fontsize=18, frameon=True, fancybox=True, shadow=False, handlelength=1.5,
               handletextpad=0.25, columnspacing=1.5, labelspacing=0.7)
    plt.tight_layout()
    plt.subplots_adjust(top=0.63)
    os.makedirs("figures", exist_ok=True)
    plt.savefig("figures/ycsb_request_reqtput.pdf", bbox_inches='tight')
    plt.savefig("figures/ycsb_request_reqtput.png", bbox_inches='tight', dpi=150)
    plt.show()


# Two-sided results (directory name is misleading: it holds the two-sided runs, 2025-11-25)
twosided_path = "outputs/ycsb_request/throughput_onesided"
# One-sided results: only complete set is in the backup (2025-11-21)
onesided_path = "outputs_backup2/ycsb_request/throughput_onesided"

data = []
for w in ["a", "b", "c"]:
    for path in [twosided_path, onesided_path]:
        df = read_data(f"{path}/{w}.csv")
        for col in ['READ_PERC', 'UPDATE_PERC', 'REQ_PER_QUERY']:
            df[col] = pd.to_numeric(df[col])
        data.append(df)
ycsb_requests(data)
ycsb_requests_speedup(data)
ycsb_requests_reqtput(data)
