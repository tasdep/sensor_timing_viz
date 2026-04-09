"""Sensor timing visualization package.

Module layout:
- ``models``: shared dataclasses and package-level types
- ``bag_io``: rosbag2 SQLite loading and time-window selection
- ``analysis``: timing statistics, gap detection, and summaries
- ``plotting``: Matplotlib figure construction
- ``reporting``: single-file HTML export
- ``args``: shared CLI/GUI parser helpers
"""

__all__ = [
    "analysis",
    "args",
    "bag_io",
    "models",
    "plotting",
    "reporting",
]
