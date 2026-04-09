import argparse
from pathlib import Path

BAG_PATH_HELP = "Path to a rosbag2 directory or a .db3 file."


def add_bag_option(parser: argparse.ArgumentParser, required: bool = False) -> None:
    parser.add_argument(
        "--bag",
        type=Path,
        required=required,
        default=None,
        help=BAG_PATH_HELP,
    )


def add_bag_positional_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("bag", type=Path, help=BAG_PATH_HELP)


def add_time_window_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", type=float, default=0.0, help="Start offset in seconds from bag start.")
    parser.add_argument("--end", type=float, default=None, help="End offset in seconds from bag start.")
