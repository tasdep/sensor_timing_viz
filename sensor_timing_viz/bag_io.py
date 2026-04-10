import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import rosbag2_py
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

from .models import TopicData


# Bag discovery and time-window helpers


def infer_storage_id(path: Path) -> str:
    if path.is_file():
        if path.suffix == ".db3":
            return "sqlite3"
        if path.suffix == ".mcap":
            return "mcap"
        raise FileNotFoundError(f"Unsupported bag file type: {path.suffix}")

    if not path.is_dir():
        raise FileNotFoundError(f"Bag path does not exist: {path}")

    metadata_path = path / "metadata.yaml"
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata.yaml not found in {path}")

    metadata_text = metadata_path.read_text(encoding="utf-8")
    match = re.search(r"^\s*storage_identifier:\s*(\S+)\s*$", metadata_text, flags=re.MULTILINE)
    if match is None:
        raise RuntimeError(f"Could not determine rosbag storage identifier from {metadata_path}")
    return match.group(1)


def open_bag_reader(path: Path):
    storage_id = infer_storage_id(path)
    reader_class = rosbag2_py.SequentialReader
    if path.is_dir():
        metadata = rosbag2_py.Info().read_metadata(str(path), storage_id)
        compression_mode = str(metadata.compression_mode).upper()
        if compression_mode not in ("", "NONE"):
            reader_class = rosbag2_py.SequentialCompressionReader

    reader = reader_class()
    reader.open(
        rosbag2_py.StorageOptions(uri=str(path), storage_id=storage_id),
        rosbag2_py.ConverterOptions("", ""),
    )
    return reader


def read_bag_metadata(path: Path):
    storage_id = infer_storage_id(path)
    if path.is_dir():
        return rosbag2_py.Info().read_metadata(str(path), storage_id)

    reader = open_bag_reader(path)
    return reader.get_metadata()


def bag_time_bounds_ns(
    bag_path: Path,
) -> Tuple[int, int]:
    metadata = read_bag_metadata(bag_path)
    bag_start_ns = int(metadata.starting_time.nanoseconds)
    bag_end_ns = bag_start_ns + int(metadata.duration.nanoseconds)
    if bag_end_ns < bag_start_ns:
        raise RuntimeError("Could not determine rosbag time bounds.")
    return bag_start_ns, bag_end_ns


def resolve_time_window_ns(
    bag_path: Path, start_offset_s: Optional[float], end_offset_s: Optional[float]
) -> Tuple[Optional[int], Optional[int], int, int]:
    bag_start_ns, bag_end_ns = bag_time_bounds_ns(bag_path)
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
    bag_path: Path,
    selected_topics: Optional[Sequence[str]],
    start_time_ns: Optional[int] = None,
    end_time_ns: Optional[int] = None,
) -> Dict[str, TopicData]:
    reader = open_bag_reader(bag_path)
    selected_set = set(selected_topics) if selected_topics else None
    topics: Dict[str, TopicData] = {}
    message_classes: Dict[str, object] = {}

    for topic_metadata in reader.get_all_topics_and_types():
        name = topic_metadata.name
        message_type = topic_metadata.type
        if selected_set is not None and name not in selected_set:
            continue
        topics.setdefault(name, TopicData(name=name, message_type=message_type))
        message_classes.setdefault(name, get_message(message_type))

    if start_time_ns is not None:
        reader.seek(start_time_ns)

    while reader.has_next():
        topic_name, payload, bag_time_ns = reader.read_next()
        bag_time_ns = int(bag_time_ns)
        if end_time_ns is not None and bag_time_ns > end_time_ns:
            break
        if selected_set is not None and topic_name not in selected_set:
            continue

        topic_data = topics[topic_name]
        topic_data.bag_times_ns.append(bag_time_ns)
        topic_data.message_count = len(topic_data.bag_times_ns)

        try:
            message = deserialize_message(payload, message_classes[topic_name])
        except Exception:
            continue

        if topic_has_header(message):
            header_time_ns = stamp_to_nanoseconds(message.header.stamp)
            if header_time_ns > 0:
                topic_data.header_times_ns.append(header_time_ns)

    return topics


def discover_topics(
    bag_path: Path, start_offset_s: Optional[float] = None, end_offset_s: Optional[float] = None
) -> Dict[str, TopicData]:
    if (start_offset_s is None or start_offset_s == 0.0) and end_offset_s is None:
        metadata = read_bag_metadata(bag_path)
        topics: Dict[str, TopicData] = {}
        for topic_info in metadata.topics_with_message_count:
            topic_metadata = topic_info.topic_metadata
            topics[topic_metadata.name] = TopicData(
                name=topic_metadata.name,
                message_type=topic_metadata.type,
                message_count=int(topic_info.message_count),
            )
        return topics

    start_ns, end_ns, _, _ = resolve_time_window_ns(bag_path, start_offset_s, end_offset_s)
    return load_topic_data(bag_path, selected_topics=None, start_time_ns=start_ns, end_time_ns=end_ns)
