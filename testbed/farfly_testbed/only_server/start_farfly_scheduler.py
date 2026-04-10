from pathlib import Path

from farfly_scheduler import start_scheduler


if __name__ == "__main__":
    start_scheduler(str(Path(__file__).resolve().with_name("farfly_scheduler_config.json")))