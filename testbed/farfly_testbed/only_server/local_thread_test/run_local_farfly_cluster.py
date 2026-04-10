from pathlib import Path
import sys
import threading
import time

BASE_DIR = Path(__file__).resolve().parent
PARENT_DIR = BASE_DIR.parent
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))

from farfly_scheduler import start_scheduler
from farfly_server import start_server


def main() -> None:
    server_configs = [
        BASE_DIR / "config_server0.json",
        BASE_DIR / "config_server1.json",
        BASE_DIR / "config_server2.json",
    ]
    for config_path in server_configs:
        worker = threading.Thread(target=start_server, args=(str(config_path),), daemon=True)
        worker.start()

    time.sleep(2.0)
    start_scheduler(str(BASE_DIR / "farfly_scheduler_config.json"))


if __name__ == "__main__":
    main()