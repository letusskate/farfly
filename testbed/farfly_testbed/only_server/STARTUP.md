# FarFly Testbed Startup Guide

## 1. Directory Role

This directory is the standalone FarFly testbed copy.

It is isolated from the legacy files under testbed/only_server such as EPARA, ADAINF, InterEdge, and other server runtime variants.

The current implementation contains:

- Scheduler: farfly_scheduler.py
- Server runtime: farfly_server.py
- Static placement file: farfly_static_placement.json
- Local single-machine three-server emulation: local_thread_test/run_local_farfly_cluster.py
- Forecast-driven experiment driver: forecast_experiment.py

## 2. Current Communication Stack

The current implementation is now based on gRPC.

Specifically:

- farfly_scheduler.py now dispatches to the gRPC implementation in farfly_scheduler_grpc.py.
- farfly_server.py now dispatches to the gRPC implementation in farfly_server_grpc.py.
- Scheduler-to-server communication uses gRPC unary RPC calls.
- Client-to-scheduler communication also uses gRPC.
- The helper script grpc_admin.py is used for health, metrics, and plan inspection.

This means the transport layer is no longer Flask HTTP/JSON.

The current target is still to validate:

- offline placement loading
- scheduling logic
- batching behavior
- queueing behavior
- credit and violation accounting
- single-machine multi-server emulation

## 3. Is gRPC the Final Choice?

Not necessarily.

gRPC is a much better fit than Flask for the current testbed stage because it replaces ad hoc HTTP endpoints with a real RPC transport.

But for real datacenter serving, even gRPC may not be the final choice if you later need lower overhead, tighter control of transport, or custom streaming semantics.

More realistic alternatives are:

1. gRPC over HTTP/2
   Good when you want structured RPC, streaming, typed interfaces, and better performance than simple Flask JSON APIs.

2. ZeroMQ or nanomsg-style messaging
   Good when you want lightweight brokerless message passing and flexible patterns such as push/pull or pub/sub.

3. Raw TCP sockets with a custom binary protocol
   Good when you want maximum control and minimum framework overhead, but development complexity is much higher.

4. FastAPI plus Uvicorn or another ASGI server
   Better than Flask operationally, but still mainly an HTTP service stack rather than a datacenter RPC system.

For this project, the most practical next upgrade path is:

1. keep the placement file and scheduling logic unchanged
2. keep the scheduler API shape roughly unchanged
3. optimize the current gRPC path first
4. later decide whether scheduler-to-server traffic should move to ZeroMQ or raw sockets

## 4. Static Placement

Placement is now offline and static by default.

The placement file is:

- farfly_static_placement.json

The scheduler config points to that file and defaults to static mode.

That means:

- placement is decided before runtime
- services stay fixed during the experiment
- you can focus on throughput, queue delay, and violations
- no online migration is required during normal runs

This matches the intended current workflow.

## 5. Python Environment

Use the Conda environment named cardstorch.

Example command prefix on this machine:

```powershell
C:/ProgramData/miniconda3/Scripts/conda.exe run -p C:\Users\15770\.conda\envs\cardstorch python ...
```

Do not run this testbed in the base environment.

## 6. Single-Machine Startup

This mode emulates three servers on one machine using three local processes plus one scheduler process.

Ports used by the local emulation:

- server0: 127.0.0.1:5000
- server1: 127.0.0.1:5001
- server2: 127.0.0.1:5002
- scheduler: 127.0.0.1:5100

Main entry:

- local_thread_test/run_local_farfly_cluster.py

Start command from the repository root:

```powershell
C:/ProgramData/miniconda3/Scripts/conda.exe run -p C:\Users\15770\.conda\envs\cardstorch python testbed/farfly_testbed/only_server/local_thread_test/run_local_farfly_cluster.py
```

What this script does:

1. starts three local server processes in threads
2. waits briefly
3. starts the local scheduler
4. loads the static placement plan
5. pushes placement to the local servers

## 7. Verify Startup

After startup, verify the scheduler:

```powershell
C:/ProgramData/miniconda3/Scripts/conda.exe run -p C:\Users\15770\.conda\envs\cardstorch python testbed/farfly_testbed/only_server/grpc_admin.py scheduler health
```

Verify the active plan:

```powershell
C:/ProgramData/miniconda3/Scripts/conda.exe run -p C:\Users\15770\.conda\envs\cardstorch python testbed/farfly_testbed/only_server/grpc_admin.py scheduler plan
```

Verify one server:

```powershell
C:/ProgramData/miniconda3/Scripts/conda.exe run -p C:\Users\15770\.conda\envs\cardstorch python testbed/farfly_testbed/only_server/grpc_admin.py server metrics --config testbed/farfly_testbed/only_server/local_thread_test/config_server0.json
```

If health returns placement_mode = static and the plan file points to farfly_static_placement.json, startup is correct.

## 8. Run a Short Experiment

Use the forecast-driven experiment driver:

```powershell
C:/ProgramData/miniconda3/Scripts/conda.exe run -p C:\Users\15770\.conda\envs\cardstorch python testbed/farfly_testbed/only_server/forecast_experiment.py --sender-config testbed/farfly_testbed/only_server/local_thread_test/config_sender.json --scheduler-config testbed/farfly_testbed/only_server/local_thread_test/farfly_scheduler_config.json --duration-seconds 2 --latency-scale 1.0 --frequency-scale 0.25 --max-workers 24
```

This prints:

- request count
- throughput
- violation count
- per-service server distribution
- scheduler metrics
- per-server metrics

## 9. Stop Local Processes

If you need to stop all local testbed processes on this machine:

```powershell
$ports = 5000,5001,5002,5100
$processIds = Get-NetTCPConnection -LocalPort $ports -State Listen -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
foreach ($processId in $processIds) { Stop-Process -Id $processId -Force }
```

## 10. Multi-Machine Startup

If you later run on multiple machines, use the top-level configs instead of local_thread_test.

Relevant files:

- config_server0.json
- config_server1.json
- config_server2.json
- config_sender.json
- farfly_scheduler_config.json

Server-specific launchers:

- dcn6_start_server.py
- dcn7_start_server.py
- dcn8_start_server.py

Scheduler launcher:

- start_farfly_scheduler.py

Recommended order:

1. start the three servers on their target machines
2. start the scheduler on the selected control node
3. verify scheduler health and plan through grpc_admin.py
4. run a sender or experiment script

## 11. What To Change First If We Upgrade Communication Again

If you decide to move away from gRPC later, the lowest-risk order is:

1. keep farfly_static_placement.json unchanged
2. keep farfly_core.py unchanged as the planning and profile layer
3. replace the ExecuteBatch gRPC path first
4. replace the scheduler-to-server placement update RPC next
5. finally replace client submission if needed

That way, placement and scheduling logic remain stable while the transport layer changes underneath.