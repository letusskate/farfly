from pathlib import Path

config_file = Path(__file__).resolve().with_name("config_server1.json")

from farfly_server import start_server

start_server(str(config_file))