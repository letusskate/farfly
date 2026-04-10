from farfly_server_grpc import FarflyServer, start_server


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start a FarFly gRPC server")
    parser.add_argument("--config", required=True, help="Path to the server config file")
    args = parser.parse_args()
    start_server(args.config)