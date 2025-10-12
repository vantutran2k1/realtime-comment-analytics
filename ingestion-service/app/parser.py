import argparse


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Youtube live comments ingestion service",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--video_id", type=str, help="Livestream video id to get comments from"
    )

    return parser
