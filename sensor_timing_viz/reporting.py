import base64
import html
import io
from pathlib import Path
from typing import List, Sequence

from matplotlib.figure import Figure

from .analysis import (
    build_bag_header_offset_table_rows,
    build_timing_diagram_summary_table_rows,
    build_variability_table_rows,
    make_timing_summary,
    summarize_variability,
)
from .models import AnalysisResult
from .plotting import render_bag_header_offset_figure, render_timing_diagram_figure, render_variability_figure


# HTML report helpers


def _figure_to_base64_png(figure: Figure) -> str:
    buffer = io.BytesIO()
    figure.savefig(buffer, format="png", bbox_inches="tight")
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def _html_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_html = "".join(f"<th>{html.escape(str(header))}</th>" for header in headers)
    body_rows: List[str] = []
    for row in rows:
        cell_html = "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row)
        body_rows.append(f"<tr>{cell_html}</tr>")
    return (
        "<table>"
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table>"
    )


# HTML report export


def export_html_report(
    result: AnalysisResult,
    output_path: Path,
    timing_diagram_summary_basis: str = "header",
    variability_basis: str = "bag",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    timing_summaries, timing_gap_map = make_timing_summary(
        topic_names=result.topic_names,
        topic_data=result.topic_data,
        timestamp_source=timing_diagram_summary_basis,
        expected_periods=result.expected_periods,
        gap_factor=result.gap_threshold_factor,
        minimum_threshold_s=result.gap_threshold_sec,
        reference_ns=result.reference_ns,
    )
    timing_rows = build_timing_diagram_summary_table_rows(timing_summaries, timing_gap_map)
    bag_header_offset_rows = build_bag_header_offset_table_rows(result.offset_summaries)

    timing_figure = render_timing_diagram_figure(result, embedded=False)
    timing_image = _figure_to_base64_png(timing_figure)

    bag_header_offset_image = None
    bag_header_offset_error = None
    try:
        bag_header_offset_figure = render_bag_header_offset_figure(result, embedded=False)
        bag_header_offset_image = _figure_to_base64_png(bag_header_offset_figure)
    except Exception as error:
        bag_header_offset_error = str(error)

    variability_sections: List[str] = []
    for topic_name in result.topic_names:
        variability_figure = render_variability_figure(
            result,
            topic_name,
            embedded=False,
            timestamp_basis=variability_basis,
        )
        variability_image = _figure_to_base64_png(variability_figure)
        variability_summary = summarize_variability(
            topic_name,
            result.topic_data[topic_name],
            variability_basis,
        )
        variability_rows = build_variability_table_rows(variability_summary)
        variability_sections.extend(
            [
                "<section>",
                f"<h2>Timing Variability: {html.escape(topic_name)} ({html.escape(variability_basis)})</h2>",
                f"<img alt='Timing variability for {html.escape(topic_name)}' src='data:image/png;base64,{variability_image}' />",
                _html_table(["Metric", "Value"], variability_rows),
                "</section>",
            ]
        )

    time_window_text = "Full bag"
    if result.start_offset_s is not None or result.end_offset_s is not None:
        start_text = f"{result.start_offset_s:.3f} s" if result.start_offset_s is not None else "bag start"
        end_text = f"{result.end_offset_s:.3f} s" if result.end_offset_s is not None else "bag end"
        time_window_text = f"{start_text} to {end_text}"

    metadata_rows = [
        ["Bag", result.bag_label],
        ["Topics", ", ".join(result.topic_names)],
        ["Time window", time_window_text],
        ["Timing Diagram display", "bag + header"],
        ["Timing Diagram gap/stat basis", result.timestamp_source],
        ["Timing Diagram summary basis", timing_diagram_summary_basis],
        ["Timing Diagram gap basis", result.timestamp_source],
        ["Variability plots", "All selected topics"],
        ["Variability basis", variability_basis],
    ]

    parts = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        "<head>",
        "<meta charset='utf-8'>",
        f"<title>{html.escape(result.title or f'Sensor Timing Report: {result.bag_label}')}</title>",
        "<style>",
        "body { font-family: Arial, sans-serif; margin: 24px; color: #1f2933; background: #f7f9fb; }",
        "h1, h2 { margin-bottom: 0.35em; }",
        "section { background: white; border: 1px solid #d9e2ec; border-radius: 10px; padding: 18px; margin-bottom: 20px; }",
        "img { max-width: 100%; height: auto; border: 1px solid #d9e2ec; border-radius: 8px; background: white; }",
        "table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 14px; }",
        "th, td { border: 1px solid #d9e2ec; padding: 8px 10px; text-align: left; vertical-align: top; }",
        "th { background: #eef2f7; }",
        ".muted { color: #52606d; }",
        ".meta { width: auto; }",
        "</style>",
        "</head>",
        "<body>",
        f"<h1>{html.escape(result.title or f'Sensor Timing Report: {result.bag_label}')}</h1>",
        f"<p class='muted'>Generated from rosbag selection: {html.escape(result.bag_label)}</p>",
        "<section>",
        "<h2>Overview</h2>",
        _html_table(["Field", "Value"], metadata_rows),
        "</section>",
        "<section>",
        "<h2>Timing Diagram</h2>",
        f"<img alt='Timing diagram' src='data:image/png;base64,{timing_image}' />",
        _html_table(
            [
                "Topic",
                "Count",
                f"Median dt [ms] ({timing_diagram_summary_basis})",
                f"Effective Rate [Hz] ({timing_diagram_summary_basis})",
                f"Max gap [s] ({timing_diagram_summary_basis})",
                f"Gap count ({timing_diagram_summary_basis})",
                f"Threshold [s] ({timing_diagram_summary_basis})",
                f"Gap windows [s] ({timing_diagram_summary_basis})",
            ],
            timing_rows,
        ),
        "</section>",
        "<section>",
        "<h2>Bag-Header Offset</h2>",
    ]

    if bag_header_offset_image is not None:
        parts.append(f"<img alt='Bag-header offset plot' src='data:image/png;base64,{bag_header_offset_image}' />")
        parts.append(
            _html_table(
                ["Topic", "Count", "Min [ms]", "Median [ms]", "P95 [ms]", "Max [ms]", "Start [ms]", "End [ms]", "Drift [ms]"],
                bag_header_offset_rows,
            )
        )
    else:
        parts.append(
            f"<p class='muted'>{html.escape(bag_header_offset_error or 'No bag-header offset data available.')}</p>"
        )
    parts.append("</section>")

    parts.extend(variability_sections)

    parts.extend(["</body>", "</html>"])
    output_path.write_text("\n".join(parts), encoding="utf-8")
