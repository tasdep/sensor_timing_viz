import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from .models import TopicData


# Bag discovery and time-window helpers


def resolve_db3_files(path: Path) -> List[Path]:
    if path.is_file():
        if path.suffix != ".db3":
            raise FileNotFoundError(f"Expected a .db3 file, got {path}")
        return [path]

    if not path.is_dir():
        raise FileNotFoundError(f"Bag path does not exist: {path}")

    db3_files = sorted(path.glob("*.db3"))
    if db3_files:
        return db3_files

    compressed_files = sorted(path.glob("*.db3.zstd"))
    if compressed_files:
        raise RuntimeError(
            "The bag only contains compressed .db3.zstd files. "
            "This tool currently needs decompressed .db3 chunks in the bag directory."
        )

    raise FileNotFoundError(f"No .db3 files found in {path}")


def bag_time_bounds_ns(db3_files: Iterable[Path]) -> Tuple[int, int]:
    min_ts: Optional[int] = None
    max_ts: Optional[int] = None
    for db3_file in db3_files:
        connection = sqlite3.connect(db3_file)
        try:
            lower, upper = connection.execute("SELECT MIN(timestamp), MAX(timestamp) FROM messages").fetchone()
        finally:
            connection.close()
        if lower is None or upper is None:
            continue
        lower = int(lower)
        upper = int(upper)
        min_ts = lower if min_ts is None else min(min_ts, lower)
        max_ts = upper if max_ts is None else max(max_ts, upper)
    if min_ts is None or max_ts is None:
        raise RuntimeError("Could not determine rosbag time bounds.")
    return min_ts, max_ts


def resolve_time_window_ns(
    db3_files: Iterable[Path], start_offset_s: Optional[float], end_offset_s: Optional[float]
) -> Tuple[Optional[int], Optional[int], int, int]:
    bag_start_ns, bag_end_ns = bag_time_bounds_ns(db3_files)
    start_ns = None if start_offset_s is None else bag_start_ns + int(start_offset_s * 1_000_000_000.0)
    end_ns = None if end_offset_s is None else bag_start_ns + int(end_offset_s * 1_000_000_000.0)
    if start_ns is not None:
        start_ns = max(bag_start_ns, start_ns)
    if end_ns is not None:
        end_ns = min(bag_end_ns, end_ns)
    if start_ns is not None and end_ns is not None and end_ns < start_ns:
        raise RuntimeError("End time must be greater than or equal to start time.")
    return start_ns, end_ns, bag_start_ns, bag_end_ns


# Message timestamp extraction


def topic_has_header(message) -> bool:
    return hasattr(message, "header") and hasattr(message.header, "stamp")


def stamp_to_nanoseconds(stamp) -> int:
    return int(stamp.sec) * 1_000_000_000 + int(stamp.nanosec)


# Topic loading


def load_topic_data(
    db3_files: Iterable[Path],
    selected_topics: Optional[Sequence[str]],
    start_time_ns: Optional[int] = None,
    end_time_ns: Optional[int] = None,
) -> Dict[str, TopicData]:
    selected_set = set(selected_topics) if selected_topics else None
    topics: Dict[str, TopicData] = {}
    message_classes: Dict[str, object] = {}

    for db3_file in db3_files:
        connection = sqlite3.connect(db3_file)
        try:
            topic_rows = connection.execute("SELECT id, name, type FROM topics").fetchall()
            topic_info = {topic_id: (name, message_type) for topic_id, name, message_type in topic_rows}

            for _, name, message_type in topic_rows:
                if selected_set is not None and name not in selected_set:
                    continue
                topics.setdefault(name, TopicData(name=name, message_type=message_type))
                message_classes.setdefault(name, get_message(message_type))

            query = "SELECT topic_id, timestamp, data FROM messages"
            params: List[int] = []
            clauses: List[str] = []
            if start_time_ns is not None:
                clauses.append("timestamp >= ?")
                params.append(start_time_ns)
            if end_time_ns is not None:
                clauses.append("timestamp <= ?")
                params.append(end_time_ns)
            if clauses:
                query += " WHERE " + " AND ".join(clauses)
            query += " ORDER BY timestamp"

            for topic_id, bag_time_ns, payload in connection.execute(query, params):
                name, message_type = topic_info[topic_id]
                if selected_set is not None and name not in selected_set:
                    continue

                topic_data = topics.setdefault(name, TopicData(name=name, message_type=message_type))
                topic_data.bag_times_ns.append(int(bag_time_ns))

                try:
                    message = deserialize_message(payload, message_classes[name])
                except Exception:
                    continue

                if topic_has_header(message):
                    header_time_ns = stamp_to_nanoseconds(message.header.stamp)
                    if header_time_ns > 0:
                        topic_data.header_times_ns.append(header_time_ns)
        finally:
            connection.close()

    return topics


def discover_topics(
    bag_path: Path, start_offset_s: Optional[float] = None, end_offset_s: Optional[float] = None
) -> Dict[str, TopicData]:
    db3_files = resolve_db3_files(bag_path)
    start_ns, end_ns, _, _ = resolve_time_window_ns(db3_files, start_offset_s, end_offset_s)
    return load_topic_data(db3_files, selected_topics=None, start_time_ns=start_ns, end_time_ns=end_ns)
