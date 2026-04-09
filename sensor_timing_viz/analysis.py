import math
from typing import Dict, List, Optional, Sequence, Tuple

from .bag_io import load_topic_data, resolve_db3_files, resolve_time_window_ns
from .models import (
    AnalysisOptions,
    AnalysisResult,
    DEFAULT_EXCLUDED_TOPICS,
    GapInterval,
    OffsetSummary,
    TopicData,
    TopicSummary,
    VariabilitySummary,
)


# Shared analysis helpers


def filter_analysis_result(result: AnalysisResult, topic_names: Sequence[str]) -> AnalysisResult:
    topic_set = set(topic_names)
    ordered_topics = [topic for topic in result.topic_names if topic in topic_set]
    filtered_topic_data = {topic: result.topic_data[topic] for topic in ordered_topics}
    filtered_offset_summaries = [summary for summary in result.offset_summaries if summary.name in topic_set]
    filtered_timing_summaries_by_basis = {
        basis: [summary for summary in summaries if summary.name in topic_set]
        for basis, summaries in result.timing_summaries_by_basis.items()
    }
    filtered_timing_gap_maps_by_basis = {
        basis: {topic: gap_map[topic] for topic in ordered_topics}
        for basis, gap_map in result.timing_gap_maps_by_basis.items()
    }
    active_basis = result.timestamp_source
    filtered_summaries = filtered_timing_summaries_by_basis[active_basis]
    filtered_gap_map = filtered_timing_gap_maps_by_basis[active_basis]

    return AnalysisResult(
        bag_label=result.bag_label,
        topic_names=ordered_topics,
        topic_data=filtered_topic_data,
        timing_summaries_by_basis=filtered_timing_summaries_by_basis,
        timing_gap_maps_by_basis=filtered_timing_gap_maps_by_basis,
        summaries=filtered_summaries,
        gap_map=filtered_gap_map,
        reference_ns=result.reference_ns,
        timestamp_source=result.timestamp_source,
        title=result.title,
        start_offset_s=result.start_offset_s,
        end_offset_s=result.end_offset_s,
        expected_periods=dict(result.expected_periods),
        gap_threshold_factor=result.gap_threshold_factor,
        gap_threshold_sec=result.gap_threshold_sec,
        figure_width=result.figure_width,
        figure_height=result.figure_height,
        dpi=result.dpi,
        offset_summaries=filtered_offset_summaries,
    )


def parse_expected_periods(values: Sequence[str]) -> Dict[str, float]:
    expected_periods: Dict[str, float] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(f"Invalid expected period '{value}'. Use TOPIC=SECONDS.")
        topic, period_text = value.split("=", 1)
        expected_periods[topic.strip()] = float(period_text)
    return expected_periods


def set_active_timing_basis(result: AnalysisResult, timestamp_source: str) -> None:
    result.timestamp_source = timestamp_source
    result.summaries = result.timing_summaries_by_basis[timestamp_source]
    result.gap_map = result.timing_gap_maps_by_basis[timestamp_source]


def default_selected_topics(topic_data: Dict[str, TopicData]) -> List[str]:
    topics = []
    for topic in sorted(topic_data):
        entry = topic_data[topic]
        if topic in DEFAULT_EXCLUDED_TOPICS:
            continue
        if len(entry.bag_times_ns) <= 1:
            continue
        topics.append(topic)
    return topics


def nanoseconds_to_relative_seconds(timestamps_ns: Sequence[int], reference_ns: int) -> List[float]:
    if not timestamps_ns:
        return []
    return [(timestamp_ns - reference_ns) / 1_000_000_000.0 for timestamp_ns in timestamps_ns]


def inter_arrival_seconds(timestamps_ns: Sequence[int]) -> List[float]:
    return [
        (timestamps_ns[index + 1] - timestamps_ns[index]) / 1_000_000_000.0
        for index in range(len(timestamps_ns) - 1)
    ]


def median(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]
    return 0.5 * (ordered[middle - 1] + ordered[middle])


def percentile(values: Sequence[float], fraction: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def standard_deviation(values: Sequence[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    avg = mean(values)
    if avg is None:
        return None
    variance = sum((value - avg) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def median_absolute_deviation(values: Sequence[float]) -> Optional[float]:
    med = median(values)
    if med is None:
        return None
    deviations = [abs(value - med) for value in values]
    return median(deviations)


def effective_rate_hz(timestamps_ns: Sequence[int]) -> Optional[float]:
    if len(timestamps_ns) < 2:
        return None
    duration_s = (timestamps_ns[-1] - timestamps_ns[0]) / 1_000_000_000.0
    if duration_s <= 0.0:
        return None
    return (len(timestamps_ns) - 1) / duration_s


def format_metric(value: Optional[float], scale: float = 1.0, precision: int = 3) -> str:
    if value is None or math.isnan(value):
        return "-"
    return f"{value * scale:.{precision}f}"


def format_rate_hz(period_s: Optional[float], precision: int = 2) -> str:
    if period_s is None or period_s <= 0.0 or math.isnan(period_s):
        return "-"
    return f"{1.0 / period_s:.{precision}f}"


def format_direct_rate_hz(rate_hz: Optional[float], precision: int = 2) -> str:
    if rate_hz is None or rate_hz <= 0.0 or math.isnan(rate_hz):
        return "-"
    return f"{rate_hz:.{precision}f}"


def timestamps_for_basis(entry: TopicData, timestamp_source: str) -> List[int]:
    if timestamp_source == "header":
        return entry.header_times_ns
    return entry.bag_times_ns


def reference_time_ns(
    topic_names: Sequence[str], topic_data: Dict[str, TopicData], timestamp_source: str
) -> int:
    candidates: List[int] = []
    for topic in topic_names:
        entry = topic_data[topic]
        if entry.bag_times_ns:
            candidates.append(entry.bag_times_ns[0])
        if entry.header_times_ns:
            candidates.append(entry.header_times_ns[0])
    if not candidates:
        raise RuntimeError("No timestamps available for the selected topics.")
    return min(candidates)


# Timing Diagram analysis


def gap_threshold_seconds(
    deltas_s: Sequence[float],
    expected_period_s: Optional[float],
    factor: float,
    minimum_threshold_s: Optional[float],
) -> Optional[float]:
    if expected_period_s is not None:
        threshold_s = expected_period_s * factor
    else:
        median_dt_s = median(deltas_s)
        if median_dt_s is None:
            return None
        threshold_s = median_dt_s * factor

    robust_floor_s = percentile(deltas_s, 0.95)
    if robust_floor_s is not None:
        threshold_s = max(threshold_s, robust_floor_s * 1.5)

    if minimum_threshold_s is not None:
        threshold_s = max(threshold_s, minimum_threshold_s)
    return threshold_s


def find_timing_gap_intervals(
    timestamps_ns: Sequence[int], threshold_s: Optional[float], reference_ns: int
) -> List[GapInterval]:
    if threshold_s is None or len(timestamps_ns) < 2:
        return []

    relative_seconds = nanoseconds_to_relative_seconds(timestamps_ns, reference_ns)
    intervals: List[GapInterval] = []
    for index in range(len(timestamps_ns) - 1):
        gap_s = (timestamps_ns[index + 1] - timestamps_ns[index]) / 1_000_000_000.0
        if gap_s > threshold_s:
            intervals.append((relative_seconds[index], relative_seconds[index + 1], gap_s))
    return intervals


def make_timing_summary(
    topic_names: Sequence[str],
    topic_data: Dict[str, TopicData],
    timestamp_source: str,
    expected_periods: Dict[str, float],
    gap_factor: float,
    minimum_threshold_s: Optional[float],
    reference_ns: int,
) -> Tuple[List[TopicSummary], Dict[str, List[GapInterval]]]:
    summaries: List[TopicSummary] = []
    gap_map: Dict[str, List[GapInterval]] = {}

    for topic in topic_names:
        entry = topic_data[topic]
        timestamps_ns = timestamps_for_basis(entry, timestamp_source)
        deltas_s = inter_arrival_seconds(timestamps_ns)
        threshold_s = gap_threshold_seconds(
            deltas_s,
            expected_periods.get(topic),
            gap_factor,
            minimum_threshold_s,
        )
        gaps = find_timing_gap_intervals(timestamps_ns, threshold_s, reference_ns)
        gap_map[topic] = gaps
        summaries.append(
            TopicSummary(
                name=topic,
                count=len(timestamps_ns),
                median_dt_s=median(deltas_s),
                effective_rate_hz=effective_rate_hz(timestamps_ns),
                max_gap_s=max(deltas_s) if deltas_s else None,
                gap_count=len(gaps),
                threshold_s=threshold_s,
            )
        )

    return summaries, gap_map


def choose_timing_diagram_plot_times(
    entry: TopicData, timestamp_source: str, reference_ns: int
) -> Dict[str, List[float]]:
    plot_times: Dict[str, List[float]] = {}
    if entry.bag_times_ns:
        plot_times["bag"] = nanoseconds_to_relative_seconds(entry.bag_times_ns, reference_ns)
    if entry.header_times_ns:
        plot_times["header"] = nanoseconds_to_relative_seconds(entry.header_times_ns, reference_ns)
    return plot_times


def format_timing_gap_windows(gaps: Sequence[GapInterval], max_items: int = 3) -> str:
    if not gaps:
        return "-"
    parts = [f"{start_s:.2f}-{end_s:.2f}" for start_s, end_s, _ in gaps[:max_items]]
    if len(gaps) > max_items:
        parts.append(f"+{len(gaps) - max_items} more")
    return ", ".join(parts)


def build_timing_diagram_summary_table_rows(
    summaries: Sequence[TopicSummary], gap_map: Dict[str, List[GapInterval]]
) -> List[List[str]]:
    rows: List[List[str]] = []
    for summary in summaries:
        rows.append(
            [
                summary.name,
                str(summary.count),
                format_metric(summary.median_dt_s, scale=1000.0, precision=1),
                format_direct_rate_hz(summary.effective_rate_hz),
                format_metric(summary.max_gap_s, precision=3),
                str(summary.gap_count),
                format_metric(summary.threshold_s, precision=3),
                format_timing_gap_windows(gap_map[summary.name]),
            ]
        )
    return rows


# Bag-Header Offset analysis


def matching_bag_and_header_times(entry: TopicData) -> Optional[Tuple[List[int], List[int]]]:
    if not entry.bag_times_ns or not entry.header_times_ns:
        return None
    if len(entry.bag_times_ns) != len(entry.header_times_ns):
        return None
    return entry.bag_times_ns, entry.header_times_ns


def bag_header_offset_rows(entry: TopicData, reference_ns: int) -> Optional[Tuple[List[float], List[float]]]:
    timestamp_pairs = matching_bag_and_header_times(entry)
    if timestamp_pairs is None:
        return None
    bag_times_ns, header_times_ns = timestamp_pairs

    times_s = nanoseconds_to_relative_seconds(bag_times_ns, reference_ns)
    offsets_ms = [(bag_ns - header_ns) / 1_000_000.0 for bag_ns, header_ns in zip(bag_times_ns, header_times_ns)]
    return times_s, offsets_ms


def summarize_bag_header_offsets(topic_names: Sequence[str], topic_data: Dict[str, TopicData]) -> List[OffsetSummary]:
    candidate_starts = [
        matching_bag_and_header_times(topic_data[topic])[0][0]
        for topic in topic_names
        if topic in topic_data and matching_bag_and_header_times(topic_data[topic]) is not None
    ]
    if not candidate_starts:
        return []

    reference_ns = min(candidate_starts)
    summaries: List[OffsetSummary] = []
    for topic in topic_names:
        rows = bag_header_offset_rows(topic_data[topic], reference_ns)
        if rows is None:
            continue
        _, offsets_ms = rows
        summaries.append(
            OffsetSummary(
                name=topic,
                count=len(offsets_ms),
                min_offset_ms=min(offsets_ms) if offsets_ms else None,
                median_offset_ms=median(offsets_ms),
                max_offset_ms=max(offsets_ms) if offsets_ms else None,
                start_offset_ms=offsets_ms[0] if offsets_ms else None,
                end_offset_ms=offsets_ms[-1] if offsets_ms else None,
                drift_ms=(offsets_ms[-1] - offsets_ms[0]) if offsets_ms else None,
                p95_offset_ms=percentile(offsets_ms, 0.95),
            )
        )
    return summaries


def build_bag_header_offset_table_rows(summaries: Sequence[OffsetSummary]) -> List[List[str]]:
    rows: List[List[str]] = []
    for summary in summaries:
        rows.append(
            [
                summary.name,
                str(summary.count),
                format_metric(summary.min_offset_ms, precision=3),
                format_metric(summary.median_offset_ms, precision=3),
                format_metric(summary.p95_offset_ms, precision=3),
                format_metric(summary.max_offset_ms, precision=3),
                format_metric(summary.start_offset_ms, precision=3),
                format_metric(summary.end_offset_ms, precision=3),
                format_metric(summary.drift_ms, precision=3),
            ]
        )
    return rows


# Timing Variability analysis


def summarize_variability(topic_name: str, entry: TopicData, timestamp_source: str) -> VariabilitySummary:
    timestamps_ns = timestamps_for_basis(entry, timestamp_source)
    dt_s = inter_arrival_seconds(timestamps_ns)
    dt_ms = [value * 1000.0 for value in dt_s]
    median_dt_ms = median(dt_ms)
    mad_dt_ms = median_absolute_deviation(dt_ms)
    threshold_dt_ms = None
    outlier_count = 0
    if median_dt_ms is not None:
        p95_dt_ms = percentile(dt_ms, 0.95)
        threshold_dt_ms = max(median_dt_ms * 3.0, (p95_dt_ms * 1.5) if p95_dt_ms is not None else median_dt_ms * 3.0)
        outlier_count = sum(1 for value in dt_ms if value > threshold_dt_ms)
    return VariabilitySummary(
        name=topic_name,
        count=len(dt_ms),
        median_dt_ms=median_dt_ms,
        mean_dt_ms=mean(dt_ms),
        effective_rate_hz=effective_rate_hz(timestamps_ns),
        std_dt_ms=standard_deviation(dt_ms),
        mad_dt_ms=mad_dt_ms,
        p95_dt_ms=percentile(dt_ms, 0.95),
        p99_dt_ms=percentile(dt_ms, 0.99),
        min_dt_ms=min(dt_ms) if dt_ms else None,
        max_dt_ms=max(dt_ms) if dt_ms else None,
        outlier_count=outlier_count,
        threshold_dt_ms=threshold_dt_ms,
    )


def variability_series(
    topic_name: str, entry: TopicData, timestamp_source: str, reference_ns: int
) -> Tuple[List[float], List[float], List[float], VariabilitySummary]:
    timestamps_ns = timestamps_for_basis(entry, timestamp_source)
    if len(timestamps_ns) < 2:
        return [], [], [], summarize_variability(topic_name, entry, timestamp_source)
    times_s = nanoseconds_to_relative_seconds(timestamps_ns[1:], reference_ns)
    dt_ms = [value * 1000.0 for value in inter_arrival_seconds(timestamps_ns)]
    summary = summarize_variability(topic_name, entry, timestamp_source)
    baseline = summary.median_dt_ms if summary.median_dt_ms is not None else 0.0
    normalized_percent = [((value - baseline) / baseline * 100.0) if baseline > 0.0 else 0.0 for value in dt_ms]
    return times_s, dt_ms, normalized_percent, summary


def build_variability_table_rows(summary: VariabilitySummary) -> List[List[str]]:
    return [
        ["Samples", str(summary.count)],
        ["Median dt [ms]", format_metric(summary.median_dt_ms, precision=3)],
        ["Effective rate [Hz]", format_direct_rate_hz(summary.effective_rate_hz)],
        ["Mean dt [ms]", format_metric(summary.mean_dt_ms, precision=3)],
        ["Mean rate [Hz]", format_rate_hz(None if summary.mean_dt_ms is None else summary.mean_dt_ms / 1000.0)],
        ["Std dev [ms]", format_metric(summary.std_dt_ms, precision=3)],
        ["MAD [ms]", format_metric(summary.mad_dt_ms, precision=3)],
        ["P95 dt [ms]", format_metric(summary.p95_dt_ms, precision=3)],
        ["P99 dt [ms]", format_metric(summary.p99_dt_ms, precision=3)],
        ["Min dt [ms]", format_metric(summary.min_dt_ms, precision=3)],
        ["Max dt [ms]", format_metric(summary.max_dt_ms, precision=3)],
        ["Outliers", str(summary.outlier_count)],
        ["Outlier threshold [ms]", format_metric(summary.threshold_dt_ms, precision=3)],
    ]


# Top-level bag analysis


def analyze_bag(options: AnalysisOptions) -> AnalysisResult:
    db3_files = resolve_db3_files(options.bag_path)
    start_ns, end_ns, _, _ = resolve_time_window_ns(db3_files, options.start_offset_s, options.end_offset_s)
    topic_data = load_topic_data(
        db3_files,
        options.selected_topics,
        start_time_ns=start_ns,
        end_time_ns=end_ns,
    )
    topic_names = list(options.selected_topics) if options.selected_topics else default_selected_topics(topic_data)

    missing_topics = [topic for topic in topic_names if topic not in topic_data]
    if missing_topics:
        raise RuntimeError("These topics were not found in the bag: " + ", ".join(missing_topics))

    reference_ns_value = reference_time_ns(topic_names, topic_data, options.timestamp_source)
    timing_summaries_by_basis: Dict[str, List[TopicSummary]] = {}
    timing_gap_maps_by_basis: Dict[str, Dict[str, List[GapInterval]]] = {}
    for basis in ("bag", "header"):
        summaries, gap_map = make_timing_summary(
            topic_names=topic_names,
            topic_data=topic_data,
            timestamp_source=basis,
            expected_periods=options.expected_periods,
            gap_factor=options.gap_threshold_factor,
            minimum_threshold_s=options.gap_threshold_sec,
            reference_ns=reference_ns_value,
        )
        timing_summaries_by_basis[basis] = summaries
        timing_gap_maps_by_basis[basis] = gap_map

    active_basis = options.timestamp_source
    timing_summaries = timing_summaries_by_basis[active_basis]
    timing_gap_map = timing_gap_maps_by_basis[active_basis]
    bag_header_offset_summaries = summarize_bag_header_offsets(topic_names, topic_data)

    return AnalysisResult(
        bag_label=options.bag_path.name,
        topic_names=topic_names,
        topic_data=topic_data,
        timing_summaries_by_basis=timing_summaries_by_basis,
        timing_gap_maps_by_basis=timing_gap_maps_by_basis,
        summaries=timing_summaries,
        gap_map=timing_gap_map,
        reference_ns=reference_ns_value,
        timestamp_source=options.timestamp_source,
        title=options.title,
        start_offset_s=options.start_offset_s,
        end_offset_s=options.end_offset_s,
        expected_periods=dict(options.expected_periods),
        gap_threshold_factor=options.gap_threshold_factor,
        gap_threshold_sec=options.gap_threshold_sec,
        figure_width=options.figure_width,
        figure_height=options.figure_height,
        dpi=options.dpi,
        offset_summaries=bag_header_offset_summaries,
    )
