# PriorityScheduleCode

[English](README.md)

## 1. 仓库介绍

这个仓库开源了一个 datacenter 场景下的 AI 服务引擎 FarFly。它面向多服务、多节点环境，核心目标是在同一套调度与执行框架里同时处理两类请求：

- frequency 请求：典型是视频流、连续帧、周期性推理，请求关注 FPS、帧间稳定性和持续服务能力。
- latency 请求：典型是交互式推理、单次调用、DDL/SLO 敏感请求，请求关注单请求响应时间和截止期是否违约。

仓库同时包含：

- FarFly 主引擎与本地 gRPC testbed
- 策略的对比入口
- Azure Functions trace 数据与适配脚本
- 若干模型仓库副本与动机实验代码

根目录下当前维护的实验入口主要是：

- `simulation/paper_simulator.py`
- `testbed/farfly_testbed/only_server/paper_benchmark.py`

## 2. 仓库文件树

下面的树按你的要求先不展开 `simulation/` 和 `deepdive/`，并且省略自动生成的 `__pycache__/`。体量较大的第三方模型子仓库和结果目录按目录级别归类说明，避免 README 变成超长清单。

```text
PriorityScheduleCode/
├─ .git/                                     # Git 历史与版本管理元数据
├─ .gitignore                                # Git 忽略规则
├─ .vscode/                                  # VS Code 工作区配置
├─ environment.yml                           # 根目录最小 conda 环境文件
├─ FarFly_v1.pdf                             # 论文/设计文档导出稿
├─ README.md                                 # 英文 README
├─ README_zh.md                              # 中文 README
├─ AI_model/                                 # 引用或保留的原始模型仓库
│  ├─ deeplabv3-plus-pytorch/                # DeepLabV3+ 原始实现与实验记录
│  ├─ deeplabv3plus_my/                      # 自定义 DeepLabV3+ 版本
│  ├─ deepseekv2-lite-instruct/              # LLM 相关资产或测试代码
│  ├─ llama3/                                # Llama 3 代码与说明
│  ├─ llama3_8B/                             # 8B 模型相关目录
│  ├─ llama3dot3_70B/                        # 70B 模型相关目录
│  ├─ mobilenetv2/                           # MobileNetV2 代码副本
│  ├─ mobilenetv2.zip                        # MobileNetV2 打包文件
│  ├─ ResNet/                                # ResNet 代码副本
│  ├─ SegFormer/                             # SegFormer 代码副本
│  ├─ unet-pytorch/                          # U-Net 代码副本
│  └─ yolov10-main/                          # YOLOv10 代码副本
├─ dataset/                                  # Azure Functions trace 与数据处理脚本
│  ├─ _analyze.py                            # trace 统计分析脚本
│  ├─ trace_adapter.py                       # Azure trace 到 FarFly 配置/事件的适配器
│  ├─ invocations_per_function_md.anon.d01.csv
│  ├─ invocations_per_function_md.anon.d02.csv  # 调用次数 trace
│  ├─ function_durations_percentiles.anon.d01.csv ... d14.csv
│  │                                          # 函数时延分位数 trace
│  ├─ app_memory_percentiles.anon.d01.csv ... d12.csv
│  │                                          # 函数内存分位数 trace
│  ├─ azurefunctions-dataset2019.tar         # Azure 数据集原始归档
│  ├─ azurefunctions-dataset2019.tar.xz      # Azure 数据集压缩归档
│  └─ cifar-10-python.tar.gz                 # 附带的数据集压缩包
├─ docker/                                   # 容器化构建文件
│  ├─ dockerfile                             # 主 Dockerfile
│  ├─ dockerfilecankao                       # 参考 Dockerfile
│  └─ dockerfileruntime                      # 运行时 Dockerfile
├─ mobilenetv2/                              # 单独保留的小型 MobileNetV2 测试目录
│  ├─ 20250221batch.md                       # batch 实验记录
│  ├─ imagenet_classes.txt                   # ImageNet 类别标签
│  ├─ test.py                                # 本地测试脚本
│  └─ test_image0.jpg                        # 测试图片
├─ motivation/                               # 动机实验与不同并行策略探索
│  ├─ batch_size/                            # batch size 动机实验
│  ├─ DP/                                    # 数据并行实验
│  ├─ model_parameter_placement_time/        # 参数放置时间实验
│  ├─ PP/                                    # 流水并行实验
│  ├─ single_GPU_multi_task/                 # 单 GPU 多任务实验
│  └─ TP/                                    # 张量并行实验
└─ testbed/                                  # 真实代码路径下的本地 testbed 
   └─ farfly_testbed/                        # FarFly testbed 主目录
      └─ only_server/                        # 当前维护的 gRPC 单机三节点 testbed
         ├─ farfly.proto                     # scheduler/server gRPC 接口定义
         ├─ farfly_pb2.py                    # protobuf 生成代码
         ├─ farfly_pb2_grpc.py               # gRPC stub 生成代码
         ├─ farfly_rpc.py                    # JSON over gRPC 序列化助手
         ├─ farfly_client.py                 # scheduler/server gRPC 客户端
         ├─ farfly_core.py                   # 服务、资源、放置、预测等核心抽象
         ├─ farfly_operators.py              # BS/MT/MF/DP/MP 算子建模
         ├─ farfly_scheduler.py              # 调度器启动封装
         ├─ farfly_scheduler_grpc.py         # FarFly 调度器主实现
         ├─ farfly_server.py                 # server 启动封装
         ├─ farfly_server_grpc.py            # FarFly server 主实现
         ├─ farfly_services.json             # 服务目录定义
         ├─ farfly_workload.json             # 工作负载定义
         ├─ farfly_static_placement.json     # 静态放置方案
         ├─ farfly_scheduler_config.json     # 默认调度器配置
         ├─ config_sender.json               # client/sender 配置
         ├─ config_server0.json
         ├─ config_server1.json
         ├─ config_server2.json              # 默认 server 配置
         ├─ forecast_experiment.py           # forecast 驱动实验脚本
         ├─ trace_experiment.py              # trace 回放实验脚本
         ├─ paper_benchmark.py               # 论文风格 benchmark 入口
         ├─ run_trace_cluster.py             # trace 配置集群启动脚本
         ├─ run_trace_benchmark.py           # trace benchmark 运行器
         ├─ run_all_benchmarks.py            # 多策略 trace benchmark 编排脚本
         ├─ grpc_admin.py                    # 健康检查、metrics、plan 查询工具
         ├─ mixed_request_input.py
         ├─ mixed_request_input2.py          # 混合请求输入生成脚本
         ├─ start_farfly_scheduler.py        # 仅启动 scheduler 的入口
         ├─ dcn6_start_server.py
         ├─ dcn7_start_server.py
         ├─ dcn8_start_server.py             # 多节点 server 启动入口
         ├─ local_thread_test/               # 单机本地 cluster 配置与启动脚本
         ├─ experiment_results/              # 历史实验结果
         ├─ paper_configs/                   # benchmark 自动生成配置
         ├─ paper_results/                   # benchmark 自动生成结果
         ├─ trace_configs/                   # trace benchmark 自动生成配置
         ├─ trace_results/                   # trace benchmark 自动生成结果
         ├─ STARTUP.md                       # testbed 启动说明
         ├─ README.md                        # 子目录说明文件
         ├─ imagenet_classes.txt             # demo 标签文件
         └─ test_image.jpg                   # demo 图片
 
```

## 3. Quick Start

### 3.1 环境需求

当前这套代码路径下，已经验证过的最小实验环境是：

- 操作系统：Windows 11
- Python：3.9.23
- 环境管理器：Conda / Miniconda / Anaconda
- 硬件：CPU 即可
- GPU / CUDA：不是必须项
- 网络：只依赖 localhost
- 端口：运行 testbed 前请确认 `9000-9800` 端口空闲

### 3.2 包依赖

当前根目录实验真正需要的核心依赖如下：

- `python = 3.9.23`
- `numpy = 2.0.1`
- `pandas = 2.3.3`
- `protobuf = 4.25.3`
- `grpcio = 1.59.5`
- `grpcio-tools = 1.59.5`（可选，仅在重新生成 proto 时需要）

建议直接使用根目录环境文件创建环境：

```powershell
conda env create -f environment.yml
conda activate priorityschedulecode
```

### 3.3 运行 testbed

在仓库根目录执行：

```powershell
python testbed/farfly_testbed/only_server/paper_benchmark.py
```

这个命令会：

- 自动生成实验配置
- 自动拉起 3 台本地 server 和 1 台 scheduler
- 顺序运行对比策略
- 默认每个策略重复 2 次，并按总 goodput / 总时间聚合

输出文件：

- `testbed/farfly_testbed/only_server/paper_results/paper_benchmark_summary.json`

常用调参方式：

```powershell
python testbed/farfly_testbed/only_server/paper_benchmark.py --frequency-scale 0.57 --latency-scale 1.0 --duration-seconds 6 --max-workers 32 --repeats 2
```

如果只想快速试跑一次：

```powershell
python testbed/farfly_testbed/only_server/paper_benchmark.py --repeats 1
```

可选地，如果你也想跑根目录模拟器，可以执行：

```powershell
python simulation/paper_simulator.py
```

结果文件：

- `simulation/paper_simulation_results.json`