import math
from pathlib import Path
from typing import Optional

from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec
from matplotlib.patches import Rectangle

from .analysis import (
    build_bag_header_offset_table_rows,
    build_variability_table_rows,
    choose_timing_diagram_plot_times,
    bag_header_offset_rows,
    summarize_variability,
    variability_series,
)
from .models import AnalysisResult


# Timing Diagram plotting


def render_timing_diagram_figure(
    result: AnalysisResult, figure: Optional[Figure] = None, embedded: bool = False
) -> Figure:
    if not result.topic_names:
        raise RuntimeError("No topics available to plot after filtering.")

    rows = len(result.topic_names)
    figure_height = result.figure_height
    if figure_height is None:
        figure_height = max(5.5, 1.8 + rows * 0.65 + 2.8)

    if figure is None:
        figure = Figure(figsize=(result.figure_width, figure_height), dpi=result.dpi, constrained_layout=False)
    else:
        figure.clear()
        figure.set_constrained_layout(False)
        if not embedded:
            figure.set_size_inches(result.figure_width, figure_height, forward=True)
            figure.set_dpi(result.dpi)

    grid = GridSpec(2, 1, figure=figure, height_ratios=[max(rows, 2), 0.9])
    axis = figure.add_subplot(grid[0])
    overview_axis = figure.add_subplot(grid[1])
    axis.set_gid("timing_main_axis")
    overview_axis.set_gid("timing_overview_axis")
    axis.set_navigate(True)
    overview_axis.set_navigate(False)

    y_positions = list(range(rows))
    axis.set_yticks(y_positions)
    axis.set_yticklabels(result.topic_names)
    axis.invert_yaxis()
    axis.grid(axis="x", linestyle="--", linewidth=0.6, alpha=0.5)
    axis.set_xlabel("Time since start of selected data [s]")
    axis.set_ylabel("Topic")
    overview_axis.set_ylabel("Overview")

    max_time_s = 0.0
    for row_index, topic in enumerate(result.topic_names):
        entry = result.topic_data[topic]
        plot_times = choose_timing_diagram_plot_times(entry, result.timestamp_source, result.reference_ns)
        time_candidates = [times[-1] for times in plot_times.values() if times]
        if time_candidates:
            max_time_s = max(max_time_s, max(time_candidates))

        for gap_start_s, gap_end_s, _ in result.gap_map[topic]:
            axis.add_patch(
                Rectangle(
                    (gap_start_s, row_index - 0.35),
                    gap_end_s - gap_start_s,
                    0.7,
                    facecolor="#d95f02",
                    alpha=0.2,
                    edgecolor="none",
                )
            )

        if "bag" in plot_times and plot_times["bag"]:
            overview_axis.scatter(
                plot_times["bag"],
                [row_index] * len(plot_times["bag"]),
                marker="|",
                s=40,
                linewidths=0.8,
                color="#1b9e77",
                alpha=0.55,
                zorder=2,
            )
            axis.scatter(
                plot_times["bag"],
                [row_index + 0.12] * len(plot_times["bag"]),
                marker="|",
                s=110,
                linewidths=1.2,
                color="#1b9e77",
                label="Bag time" if row_index == 0 else None,
                zorder=3,
            )

        if "header" in plot_times and plot_times["header"]:
            overview_axis.scatter(
                plot_times["header"],
                [row_index] * len(plot_times["header"]),
                marker=".",
                s=6,
                linewidths=0,
                color="#7570b3",
                alpha=0.35,
                zorder=1,
            )
            axis.scatter(
                plot_times["header"],
                [row_index - 0.12] * len(plot_times["header"]),
                marker="o",
                s=9,
                linewidths=0,
                color="#7570b3",
                alpha=0.8,
                label="Header time" if row_index == 0 else None,
                zorder=4,
            )

    axis.legend(loc="upper right")

    axis.set_xlim(left=0.0, right=max_time_s * 1.02 if max_time_s > 0.0 else 1.0)
    overview_axis.set_xlim(left=0.0, right=max_time_s * 1.02 if max_time_s > 0.0 else 1.0)
    axis.set_title(result.title or f"Sensor Timing Diagram: {result.bag_label}")
    overview_axis.set_yticks([])
    overview_axis.grid(axis="x", linestyle="--", linewidth=0.5, alpha=0.35)
    overview_axis.set_xlabel("Full bag time [s]")

    figure.subplots_adjust(left=0.22, right=0.985, top=0.94, bottom=0.12, hspace=0.18)

    return figure


# Bag-Header Offset plotting


def render_bag_header_offset_figure(
    result: AnalysisResult, figure: Optional[Figure] = None, embedded: bool = False
) -> Figure:
    if figure is None:
        figure = Figure(figsize=(result.figure_width, 8.0), dpi=result.dpi, constrained_layout=False)
    else:
        figure.clear()
        figure.set_constrained_layout(False)
        if not embedded:
            figure.set_size_inches(result.figure_width, 8.0, forward=True)
            figure.set_dpi(result.dpi)

    grid = GridSpec(2, 1, figure=figure, height_ratios=[3.0, 1.6])
    axis = figure.add_subplot(grid[0])
    table_axis = figure.add_subplot(grid[1])

    colors = [
        "#1b9e77",
        "#d95f02",
        "#7570b3",
        "#e7298a",
        "#66a61e",
        "#e6ab02",
        "#a6761d",
        "#666666",
    ]

    candidate_starts = [
        result.topic_data[topic].bag_times_ns[0]
        for topic in result.topic_names
        if result.topic_data[topic].bag_times_ns and result.topic_data[topic].header_times_ns
        and len(result.topic_data[topic].bag_times_ns) == len(result.topic_data[topic].header_times_ns)
    ]
    if not candidate_starts:
        raise RuntimeError("No topics in the current selection have matching bag and header timestamps.")
    offset_reference_ns = min(candidate_starts)

    plotted = 0
    for index, topic in enumerate(result.topic_names):
        rows = bag_header_offset_rows(result.topic_data[topic], offset_reference_ns)
        if rows is None:
            continue
        times_s, offsets_ms = rows
        axis.plot(
            times_s,
            offsets_ms,
            linewidth=1.1,
            color=colors[index % len(colors)],
            label=topic,
        )
        plotted += 1

    if plotted == 0:
        raise RuntimeError("No topics in the current selection have matching bag and header timestamps.")

    axis.grid(True, linestyle="--", linewidth=0.6, alpha=0.5)
    axis.set_xlabel("Time since start of selected data [s]")
    axis.set_ylabel("Bag - Header [ms]")
    axis.set_title(result.title or f"Bag-Header Offset: {result.bag_label}")
    axis.legend(loc="upper right", fontsize=8)

    table_axis.axis("off")
    table = table_axis.table(
        cellText=build_bag_header_offset_table_rows(result.offset_summaries),
        colLabels=[
            "Topic",
            "Count",
            "Min [ms]",
            "Median [ms]",
            "P95 [ms]",
            "Max [ms]",
            "Start [ms]",
            "End [ms]",
            "Drift [ms]",
        ],
        loc="center",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1.0, 1.25)
    figure.subplots_adjust(left=0.11, right=0.985, top=0.94, bottom=0.05, hspace=0.2)

    return figure


# Timing Variability plotting


def render_variability_figure(
    result: AnalysisResult,
    topic_name: str,
    figure: Optional[Figure] = None,
    embedded: bool = False,
    timestamp_basis: str = "bag",
) -> Figure:
    if topic_name not in result.topic_data:
        raise RuntimeError(f"Topic '{topic_name}' is not available in the current analysis.")

    entry = result.topic_data[topic_name]
    times_s, dt_ms, normalized_percent, summary = variability_series(
        topic_name,
        entry,
        timestamp_basis,
        result.reference_ns,
    )

    if figure is None:
        figure = Figure(figsize=(result.figure_width, 8.0), dpi=result.dpi, constrained_layout=False)
    else:
        figure.clear()
        figure.set_constrained_layout(False)
        if not embedded:
            figure.set_size_inches(result.figure_width, 8.0, forward=True)
            figure.set_dpi(result.dpi)

    grid = GridSpec(3, 2, figure=figure, height_ratios=[2.0, 2.0, 1.6], width_ratios=[3.0, 1.4])
    dt_axis = figure.add_subplot(grid[0, 0])
    dev_axis = figure.add_subplot(grid[1, 0], sharex=dt_axis)
    hist_axis = figure.add_subplot(grid[0:2, 1])
    table_axis = figure.add_subplot(grid[2, :])

    dt_axis.plot(times_s, dt_ms, color="#1b9e77", linewidth=1.0)
    if summary.median_dt_ms is not None:
        dt_axis.axhline(summary.median_dt_ms, color="#444444", linestyle="--", linewidth=0.9, alpha=0.7)
    if summary.threshold_dt_ms is not None:
        dt_axis.axhline(summary.threshold_dt_ms, color="#d95f02", linestyle=":", linewidth=0.9, alpha=0.8)
    dt_axis.set_ylabel("dt [ms]")
    dt_axis.set_title(f"Timing Variability: {topic_name} ({timestamp_basis})")
    dt_axis.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)

    dev_axis.plot(times_s, normalized_percent, color="#7570b3", linewidth=1.0)
    dev_axis.axhline(0.0, color="#444444", linestyle="--", linewidth=0.9, alpha=0.7)
    dev_axis.set_ylabel("Deviation [%]")
    dev_axis.set_xlabel("Time since start of selected data [s]")
    dev_axis.grid(True, linestyle="--", linewidth=0.5, alpha=0.4)

    bins = min(60, max(10, int(math.sqrt(len(dt_ms))) if dt_ms else 10))
    hist_axis.hist(dt_ms, bins=bins, color="#66a61e", alpha=0.8, edgecolor="white")
    if summary.median_dt_ms is not None:
        hist_axis.axvline(summary.median_dt_ms, color="#444444", linestyle="--", linewidth=0.9, alpha=0.7)
    hist_axis.set_xlabel("dt [ms]")
    hist_axis.set_ylabel("Count")
    hist_axis.set_title("Distribution")
    hist_axis.grid(True, linestyle="--", linewidth=0.5, alpha=0.3)

    table_axis.axis("off")
    table = table_axis.table(
        cellText=build_variability_table_rows(summary),
        colLabels=["Metric", "Value"],
        loc="center",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1.0, 1.2)

    figure.subplots_adjust(left=0.09, right=0.985, top=0.94, bottom=0.06, hspace=0.35, wspace=0.25)
    return figure


# Output helpers


def save_timing_diagram_figure(result: AnalysisResult, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure = render_timing_diagram_figure(result, embedded=False)
    figure.savefig(output_path)
