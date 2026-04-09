import argparse
from pathlib import Path

from .args import add_bag_positional_argument, add_time_window_args
from .analysis import analyze_bag, format_metric, format_timing_gap_windows, make_timing_summary, parse_expected_periods
from .models import AnalysisOptions
from .plotting import save_timing_diagram_figure
from .reporting import export_html_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a sensor timing diagram from a rosbag2 SQLite bag.")
    add_bag_positional_argument(parser)
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("sensor_timing_diagram.png"),
        help="Output figure path.",
    )
    parser.add_argument(
        "--html-report",
        type=Path,
        default=None,
        help="Optional output path for a single-file HTML report.",
    )
    parser.add_argument("--topics", nargs="+", help="Explicit list of topics to include.")
    parser.add_argument(
        "--timing-diagram-summary-basis",
        choices=("bag", "header"),
        default="header",
        help="Timestamp basis to use for Timing Diagram summary statistics in CLI output and the HTML report.",
    )
    add_time_window_args(parser)
    parser.add_argument("--gap-threshold-factor", type=float, default=3.0)
    parser.add_argument("--gap-threshold-sec", type=float, default=None)
    parser.add_argument(
        "--expected-period",
        action="append",
        default=[],
        metavar="TOPIC=SECONDS",
        help="Override expected period for a topic. Can be passed multiple times.",
    )
    parser.add_argument("--title", default=None)
    parser.add_argument("--figure-width", type=float, default=16.0)
    parser.add_argument("--figure-height", type=float, default=None)
    parser.add_argument("--dpi", type=int, default=150)
    parser.add_argument(
        "--variability-basis",
        choices=("bag", "header"),
        default="header",
        help="Timestamp basis to use for Timing Variability sections in the HTML report.",
    )
    return parser.parse_args()


def build_analysis_options(args: argparse.Namespace) -> AnalysisOptions:
    return AnalysisOptions(
        bag_path=args.bag,
        selected_topics=list(args.topics) if args.topics else None,
        timestamp_source=args.timing_diagram_summary_basis,
        start_offset_s=args.start,
        end_offset_s=args.end,
        gap_threshold_factor=args.gap_threshold_factor,
        gap_threshold_sec=args.gap_threshold_sec,
        expected_periods=parse_expected_periods(args.expected_period),
        title=args.title,
        figure_width=args.figure_width,
        figure_height=args.figure_height,
        dpi=args.dpi,
    )


def main() -> None:
    args = parse_args()
    options = build_analysis_options(args)
    result = analyze_bag(options)
    save_timing_diagram_figure(result, args.output)
    if args.html_report is not None:
        export_html_report(
            result,
            args.html_report,
            timing_diagram_summary_basis=args.timing_diagram_summary_basis,
            variability_basis=args.variability_basis,
        )

    print(f"Wrote timing diagram to {args.output}")
    if args.html_report is not None:
        print(f"Wrote HTML report to {args.html_report}")
    timing_summaries, timing_gap_map = make_timing_summary(
        topic_names=result.topic_names,
        topic_data=result.topic_data,
        timestamp_source=args.timing_diagram_summary_basis,
        expected_periods=options.expected_periods,
        gap_factor=options.gap_threshold_factor,
        minimum_threshold_s=options.gap_threshold_sec,
        reference_ns=result.reference_ns,
    )
    print(f"Timing Diagram summary basis: {args.timing_diagram_summary_basis}")
    for summary in timing_summaries:
        print(
            f"{summary.name}: count={summary.count}, "
            f"median_dt_ms={format_metric(summary.median_dt_s, scale=1000.0, precision=1)}, "
            f"max_gap_s={format_metric(summary.max_gap_s, precision=3)}, "
            f"gap_count={summary.gap_count}, "
            f"threshold_s={format_metric(summary.threshold_s, precision=3)}, "
            f"gap_windows={format_timing_gap_windows(timing_gap_map[summary.name], max_items=10)}"
        )


if __name__ == "__main__":
    main()
