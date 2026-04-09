from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_EXCLUDED_TOPICS = {"/tf", "/tf_static", "/robot_description"}
GapInterval = Tuple[float, float, float]


@dataclass
class TopicData:
    name: str
    message_type: str
    bag_times_ns: List[int] = field(default_factory=list)
    header_times_ns: List[int] = field(default_factory=list)


@dataclass
class TopicSummary:
    name: str
    count: int
    median_dt_s: Optional[float]
    effective_rate_hz: Optional[float]
    max_gap_s: Optional[float]
    gap_count: int
    threshold_s: Optional[float]


@dataclass
class OffsetSummary:
    name: str
    count: int
    min_offset_ms: Optional[float]
    median_offset_ms: Optional[float]
    max_offset_ms: Optional[float]
    start_offset_ms: Optional[float]
    end_offset_ms: Optional[float]
    drift_ms: Optional[float]
    p95_offset_ms: Optional[float]


@dataclass
class VariabilitySummary:
    name: str
    count: int
    median_dt_ms: Optional[float]
    mean_dt_ms: Optional[float]
    effective_rate_hz: Optional[float]
    std_dt_ms: Optional[float]
    mad_dt_ms: Optional[float]
    p95_dt_ms: Optional[float]
    p99_dt_ms: Optional[float]
    min_dt_ms: Optional[float]
    max_dt_ms: Optional[float]
    outlier_count: int
    threshold_dt_ms: Optional[float]


@dataclass
class AnalysisOptions:
    bag_path: Path
    selected_topics: Optional[List[str]] = None
    timestamp_source: str = "both"
    start_offset_s: Optional[float] = None
    end_offset_s: Optional[float] = None
    gap_threshold_factor: float = 3.0
    gap_threshold_sec: Optional[float] = None
    expected_periods: Dict[str, float] = field(default_factory=dict)
    title: Optional[str] = None
    figure_width: float = 16.0
    figure_height: Optional[float] = None
    dpi: int = 150


@dataclass
class AnalysisResult:
    bag_label: str
    topic_names: List[str]
    topic_data: Dict[str, TopicData]
    timing_summaries_by_basis: Dict[str, List[TopicSummary]]
    timing_gap_maps_by_basis: Dict[str, Dict[str, List[GapInterval]]]
    summaries: List[TopicSummary]
    gap_map: Dict[str, List[GapInterval]]
    reference_ns: int
    timestamp_source: str
    title: Optional[str]
    start_offset_s: Optional[float]
    end_offset_s: Optional[float]
    expected_periods: Dict[str, float]
    gap_threshold_factor: float
    gap_threshold_sec: Optional[float]
    figure_width: float
    figure_height: Optional[float]
    dpi: int
    offset_summaries: List[OffsetSummary]
