from farfly_scheduler_grpc import FarflyScheduler, start_scheduler


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start the FarFly gRPC control plane")
    parser.add_argument("--config", required=True, help="Path to the scheduler config file")
    args = parser.parse_args()
    start_scheduler(args.config)