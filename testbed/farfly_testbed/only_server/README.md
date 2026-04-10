FarFly testbed standalone copy.

This directory is an isolated copy of the FarFly-related scheduler, server, config, and experiment scripts.

Key points:
- Original files under testbed/only_server are left in place.
- Existing server runtime variants, EPARA, ADAINF, InterEdge, and other legacy files are not modified by this copy.
- Static placement is defined in farfly_static_placement.json.
- Runtime scheduler config defaults to static placement mode.
- Runtime transport now uses gRPC instead of Flask HTTP.
- Single-machine startup and verification steps are documented in STARTUP.md.

Files to start with:
- STARTUP.md: complete startup guide for local and multi-machine runs.
- farfly_static_placement.json: offline static placement plan.
- local_thread_test/run_local_farfly_cluster.py: single-machine three-server emulation entry.
- start_farfly_scheduler.py: scheduler-only entry.
- grpc_admin.py: gRPC health, metrics, and plan inspection helper.