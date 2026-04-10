# PriorityScheduleCode

[中文](README_zh.md)

## 1. Repository Overview

This repository open-sources a datacenter AI service engine named FarFly. The engine is designed for multi-service, multi-node environments and can handle two request classes in one scheduling and execution framework:

- frequency requests: stream-like or video-style requests that care about FPS, inter-frame stability, and sustained service quality
- latency requests: interactive or on-demand requests that care about per-request deadline/SLO satisfaction

The repository includes:

- the FarFly engine and its local gRPC testbed
- comparison baselines
- Azure Functions trace data and adaptation scripts
- model repository snapshots and motivation/ablation experiments

The main experiment entry points at the repository root are:

- `simulation/paper_simulator.py`
- `testbed/farfly_testbed/only_server/paper_benchmark.py`

## 2. Repository Tree

The tree below intentionally does not expand `simulation/` and `deepdive/` as requested, and it also omits auto-generated `__pycache__/` directories. Large third-party model subtrees and result directories are grouped at the folder level so the README stays readable.

```text
PriorityScheduleCode/
├─ .git/                                     # Git history and repository metadata
├─ .gitignore                                # Git ignore rules
├─ .vscode/                                  # VS Code workspace settings
├─ environment.yml                           # Minimal conda environment for root-level experiments
├─ FarFly_v1.pdf                             # Paper or design manuscript export
├─ README.md                                 # English README
├─ README_zh.md                              # Chinese README
├─ AI_model/                                 # Original model repositories kept in the repo
│  ├─ deeplabv3-plus-pytorch/                # DeepLabV3+ source tree and notes
│  ├─ deeplabv3plus_my/                      # Customized DeepLabV3+ variant
│  ├─ deepseekv2-lite-instruct/              # LLM-related assets or experiments
│  ├─ llama3/                                # Llama 3 code and examples
│  ├─ llama3_8B/                             # 8B model directory
│  ├─ llama3dot3_70B/                        # 70B model directory
│  ├─ mobilenetv2/                           # MobileNetV2 source copy
│  ├─ mobilenetv2.zip                        # Archived model package
│  ├─ ResNet/                                # ResNet source copy
│  ├─ SegFormer/                             # SegFormer source copy
│  ├─ unet-pytorch/                          # U-Net source copy
│  └─ yolov10-main/                          # YOLOv10 source copy
├─ dataset/                                  # Azure Functions trace and dataset utilities
│  ├─ _analyze.py                            # Trace statistics and candidate analysis
│  ├─ trace_adapter.py                       # Azure trace to FarFly config/event adapter
│  ├─ invocations_per_function_md.anon.d01.csv
│  ├─ invocations_per_function_md.anon.d02.csv  # Invocation traces
│  ├─ function_durations_percentiles.anon.d01.csv ... d14.csv
│  │                                          # Duration percentile traces
│  ├─ app_memory_percentiles.anon.d01.csv ... d12.csv
│  │                                          # Memory percentile traces
│  ├─ azurefunctions-dataset2019.tar         # Raw Azure dataset archive
│  ├─ azurefunctions-dataset2019.tar.xz      # Compressed Azure dataset archive
│  └─ cifar-10-python.tar.gz                 # Auxiliary dataset archive
├─ docker/                                   # Container build files
│  ├─ dockerfile                             # Main Dockerfile
│  ├─ dockerfilecankao                       # Reference Dockerfile
│  └─ dockerfileruntime                      # Runtime-only Dockerfile
├─ mobilenetv2/                              # Standalone lightweight MobileNetV2 test assets
│  ├─ 20250221batch.md                       # Batch experiment notes
│  ├─ imagenet_classes.txt                   # ImageNet labels
│  ├─ test.py                                # Local test script
│  └─ test_image0.jpg                        # Sample input image
├─ motivation/                               # Motivation experiments and parallelism studies
│  ├─ batch_size/                            # Batch-size studies
│  ├─ DP/                                    # Data-parallel studies
│  ├─ model_parameter_placement_time/        # Parameter placement timing studies
│  ├─ PP/                                    # Pipeline-parallel studies
│  ├─ single_GPU_multi_task/                 # Single-GPU multi-task studies
│  └─ TP/                                    # Tensor-parallel studies
└─ testbed/                                  # Local testbeds and legacy baseline code
   └─ farfly_testbed/                        # Main FarFly testbed directory
      └─ only_server/                        # Current maintained gRPC single-machine testbed
         ├─ farfly.proto                     # Scheduler/server gRPC API
         ├─ farfly_pb2.py                    # Generated protobuf messages
         ├─ farfly_pb2_grpc.py               # Generated gRPC stubs
         ├─ farfly_rpc.py                    # JSON-over-gRPC serialization helpers
         ├─ farfly_client.py                 # Scheduler/server gRPC clients
         ├─ farfly_core.py                   # Service, resource, placement, and forecast abstractions
         ├─ farfly_operators.py              # BS/MT/MF/DP/MP operator modeling
         ├─ farfly_scheduler.py              # Scheduler launcher wrapper
         ├─ farfly_scheduler_grpc.py         # Main FarFly scheduler implementation
         ├─ farfly_server.py                 # Server launcher wrapper
         ├─ farfly_server_grpc.py            # Main FarFly server implementation
         ├─ farfly_services.json             # Service catalog
         ├─ farfly_workload.json             # Workload definition
         ├─ farfly_static_placement.json     # Static placement plan
         ├─ farfly_scheduler_config.json     # Default scheduler config
         ├─ config_sender.json               # Client/sender config
         ├─ config_server0.json
         ├─ config_server1.json
         ├─ config_server2.json              # Default server configs
         ├─ forecast_experiment.py           # Forecast-driven experiment script
         ├─ trace_experiment.py              # Trace replay experiment script
         ├─ paper_benchmark.py               # Paper-style benchmark entry point
         ├─ run_trace_cluster.py             # Trace-configured local cluster launcher
         ├─ run_trace_benchmark.py           # Trace benchmark runner
         ├─ run_all_benchmarks.py            # Multi-policy trace orchestrator
         ├─ grpc_admin.py                    # Health, metrics, and plan inspection helper
         ├─ mixed_request_input.py
         ├─ mixed_request_input2.py          # Mixed workload input generators
         ├─ start_farfly_scheduler.py        # Scheduler-only startup entry
         ├─ dcn6_start_server.py
         ├─ dcn7_start_server.py
         ├─ dcn8_start_server.py             # Multi-node server startup wrappers
         ├─ local_thread_test/               # Single-machine local cluster configs and launcher
         ├─ experiment_results/              # Historical experiment outputs
         ├─ paper_configs/                   # Auto-generated paper benchmark configs
         ├─ paper_results/                   # Auto-generated paper benchmark results
         ├─ trace_configs/                   # Auto-generated trace configs
         ├─ trace_results/                   # Auto-generated trace results
         ├─ STARTUP.md                       # Standalone startup guide
         ├─ README.md                        # Subdirectory-specific notes
         ├─ imagenet_classes.txt             # Demo labels
         └─ test_image.jpg                   # Demo image

```

## 3. Quick Start

### 3.1 Environment Requirements

The currently validated minimum environment for this code path is:

- OS: Windows 11
- Python: 3.9.23
- Environment manager: Conda / Miniconda / Anaconda
- Hardware: CPU-only is enough
- GPU / CUDA: not required
- Network: localhost only
- Ports: make sure the `9000-9800` range is free before running the testbed

### 3.2 Package Requirements

The root-level experiments only need a small set of core packages:

- `python = 3.9.23`
- `numpy = 2.0.1`
- `pandas = 2.3.3`
- `protobuf = 4.25.3`
- `grpcio = 1.59.5`
- `grpcio-tools = 1.59.5` (optional, only needed when regenerating proto bindings)

The recommended setup is to create the environment from the root-level file:

```powershell
conda env create -f environment.yml
conda activate priorityschedulecode
```

### 3.3 Run the Testbed

From the repository root, run:

```powershell
python testbed/farfly_testbed/only_server/paper_benchmark.py
```

This command will:

- automatically generate experiment configs
- automatically start 3 local servers and 1 scheduler
- run baselines sequentially
- repeat each policy twice by default and aggregate total goodput over total runtime

Output file:

- `testbed/farfly_testbed/only_server/paper_results/paper_benchmark_summary.json`

Commonly adjusted command:

```powershell
python testbed/farfly_testbed/only_server/paper_benchmark.py --frequency-scale 0.57 --latency-scale 1.0 --duration-seconds 6 --max-workers 32 --repeats 2
```

For a quicker smoke test:

```powershell
python testbed/farfly_testbed/only_server/paper_benchmark.py --repeats 1
```

Optionally, if you also want to run the simulator, use:

```powershell
python simulation/paper_simulator.py
```

Output file:

- `simulation/paper_simulation_results.json`