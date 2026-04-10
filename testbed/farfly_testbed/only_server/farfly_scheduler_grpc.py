from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import heapq
import itertools
from pathlib import Path
import random as _random
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import uuid

import grpc

import farfly_pb2
import farfly_pb2_grpc
from farfly_core import (
    PlacementPlan,
    PlacementPlanner,
    ServerResource,
    ServerState,
    ServiceProfile,
    WorkloadForecast,
    load_json_file,
    load_service_catalog,
    load_workload_forecast,
    resolve_relative_path,
)
from farfly_operators import OperatorConfig, TaskResourceAllocator
from farfly_rpc import grpc_target, make_json_reply, make_json_request, parse_json_reply, parse_json_request


@dataclass
class PendingRequest:
    request_id: str
    service: str
    task_type: str
    created_at: float
    deadline_at: float
    payload: Dict[str, Any]
    flow_id: str
    frame_id: Optional[int]
    event: threading.Event = field(default_factory=threading.Event)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class ServiceAQMState:
    """Per-service Active Queue Management state at the handler (scheduler)."""
    service_name: str
    task_type: str
    heap: List[Tuple[float, int, PendingRequest]] = field(default_factory=list)
    ewma_delay_ms: float = 0.0
    alpha: float = 0.12
    target_queue_len: int = 32
    total_enqueued: int = 0
    total_dispatched: int = 0

    def push(self, pending: PendingRequest, seq: int) -> None:
        heapq.heappush(self.heap, (pending.deadline_at, seq, pending))
        self.total_enqueued += 1

    @property
    def is_empty(self) -> bool:
        return not self.heap

    @property
    def peek_deadline(self) -> float:
        return self.heap[0][0] if self.heap else float("inf")

    @property
    def queue_len(self) -> int:
        return len(self.heap)

    def update_delay(self, delay_ms: float) -> None:
        self.ewma_delay_ms = (1 - self.alpha) * self.ewma_delay_ms + self.alpha * delay_ms


class FarflyScheduler:
    def __init__(self, config_file: str) -> None:
        self.config_path = Path(config_file).resolve()
        self.config = load_json_file(self.config_path)

        self.ip = str(self.config["self"]["ip"])
        self.port = int(self.config["self"]["port"])
        self.exposed_ip = str(self.config["self"].get("exposed_ip", self.ip))

        self.window_seconds = float(self.config.get("planner", {}).get("window_seconds", 60.0))
        self.dispatch_timeout_seconds = float(self.config.get("dispatch_timeout_seconds", 15.0))
        self.violation_alpha = float(self.config.get("violation_alpha", 0.2))
        self.dispatch_workers = int(self.config.get("dispatch_workers", 8))
        self.rpc_workers = int(self.config.get("rpc_workers", max(8, self.dispatch_workers)))
        self.priority_services = list(self.config.get("planner", {}).get("priority_services", []))
        placement_config = dict(self.config.get("placement", {}))
        self.placement_mode = str(placement_config.get("mode", "planner"))
        self.allow_runtime_recompute = bool(
            placement_config.get("allow_runtime_recompute", self.placement_mode != "static")
        )
        placement_file = placement_config.get("plan_file")
        self.placement_plan_path = (
            resolve_relative_path(self.config_path, str(placement_file)) if placement_file else None
        )

        self.service_catalog: Dict[str, ServiceProfile] = load_service_catalog(self.config_path, self.config)
        self.workload_forecasts: List[WorkloadForecast] = load_workload_forecast(self.config_path, self.config)
        self.servers: Dict[str, ServerResource] = {
            server.name: server for server in (ServerResource.from_dict(item) for item in self.config.get("servers", []))
        }

        self.server_violation_estimates = {name: 0.0 for name in self.servers}
        self.server_credits = {name: resource.initial_credit_pool for name, resource in self.servers.items()}
        self.server_last_seen = {name: 0.0 for name in self.servers}
        self.service_server_offsets: Dict[str, int] = {}
        # Per-server per-service runtime metrics reported by servers
        self.server_service_metrics: Dict[str, Dict[str, Any]] = {}
        # Operator configs resolved for current placement
        self.operator_configs: Dict[Tuple[str, str], OperatorConfig] = {}

        self.plan = self._load_initial_plan()

        self.server_channels = {
            name: grpc.insecure_channel(grpc_target(server.ip, server.port))
            for name, server in self.servers.items()
        }
        self.server_stubs = {
            name: farfly_pb2_grpc.ServerServiceStub(channel)
            for name, channel in self.server_channels.items()
        }

        self.condition = threading.Condition()
        self.sequence = itertools.count()
        # Per-service AQM heaps (replaces global latency_heap / frequency_heap)
        self.service_aqm: Dict[str, ServiceAQMState] = {}
        self.running = True

        self.total_submitted = 0
        self.total_completed = 0
        self.total_violations = 0
        self.total_failed = 0

        self.credit_poll_interval = float(self.config.get("credit_poll_interval", 3.0))

        # Dispatch policy: "farfly" (default), "round_robin", "random", "least_violation"
        self.dispatch_policy = str(self.config.get("dispatch_policy", "farfly"))

        self.dispatch_executor = ThreadPoolExecutor(max_workers=max(1, self.dispatch_workers))

        dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True)
        dispatcher.start()
        # Independent credit assigner thread
        credit_thread = threading.Thread(target=self._credit_assigner_loop, daemon=True)
        credit_thread.start()

    def health_payload(self) -> Dict[str, Any]:
        return {
            "status": "ok",
            "placement_mode": self.placement_mode,
            "placement_plan_file": str(self.placement_plan_path) if self.placement_plan_path else None,
            "plan_objective": self.plan.objective_value,
        }

    def metrics_payload(self) -> Dict[str, Any]:
        with self.condition:
            per_service_queue: Dict[str, int] = {}
            for svc_name, aqm in self.service_aqm.items():
                per_service_queue[svc_name] = aqm.queue_len
            return {
                "scheduler": f"{self.exposed_ip}:{self.port}",
                "placement_mode": self.placement_mode,
                "placement_plan_file": str(self.placement_plan_path) if self.placement_plan_path else None,
                "plan": self.plan.to_dict(),
                "server_credits": self.server_credits,
                "server_violation_estimates": self.server_violation_estimates,
                "per_service_queue": per_service_queue,
                "total_submitted": self.total_submitted,
                "total_completed": self.total_completed,
                "total_violations": self.total_violations,
                "total_failed": self.total_failed,
            }

    def plan_payload(self) -> Dict[str, Any]:
        return self.plan.to_dict()

    def recompute_plan_payload(self) -> Tuple[Dict[str, Any], int]:
        if not self.allow_runtime_recompute:
            return {
                "status": "ignored",
                "reason": "scheduler is running in static placement mode",
                "plan": self.plan.to_dict(),
            }, 409
        self.plan = self._recompute_plan(push_plan=True)
        return self.plan.to_dict(), 200

    def reload_plan_payload(self) -> Tuple[Dict[str, Any], int]:
        if self.placement_mode != "static":
            return {"status": "ignored", "reason": "scheduler is not using a static placement file"}, 409
        self.plan = self._load_static_plan(push_plan=True)
        return self.plan.to_dict(), 200

    def submit_payload(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        service_name = str(payload.get("service", payload.get("service_name", "")))
        if service_name not in self.service_catalog:
            return {"status": "failed", "reason": f"unknown service {service_name}"}, 404

        profile = self.service_catalog[service_name]
        task_type = str(payload.get("task_type", profile.task_type))
        request_id = str(payload.get("request_id", uuid.uuid4()))
        created_at = float(payload.get("timestamp", time.time()))
        timeout_ms = float(payload.get("timeout_ms", profile.timeout_ms))
        deadline_at = float(payload.get("deadline_at", created_at + timeout_ms / 1000.0))
        flow_id = str(payload.get("flow_id", payload.get("stream_id", request_id)))
        frame_id = payload.get("frame_id")
        body = dict(payload.get("payload", payload.get("data", {})))

        pending = PendingRequest(
            request_id=request_id,
            service=service_name,
            task_type=task_type,
            created_at=created_at,
            deadline_at=deadline_at,
            payload=body,
            flow_id=flow_id,
            frame_id=frame_id,
        )

        wait_timeout = max(timeout_ms / 1000.0 * 2.0, self.dispatch_timeout_seconds)
        with self.condition:
            aqm = self.service_aqm.get(service_name)
            if aqm is None:
                aqm = ServiceAQMState(
                    service_name=service_name,
                    task_type=task_type,
                    target_queue_len=int(self.config.get("aqm_target_queue_len", 32)),
                )
                self.service_aqm[service_name] = aqm
            aqm.push(pending, next(self.sequence))
            self.total_submitted += 1
            self.condition.notify_all()

        if not pending.event.wait(timeout=wait_timeout):
            with self.condition:
                self.total_failed += 1
            return {"status": "failed", "reason": "scheduler timeout", "request_id": request_id}, 504

        if pending.error is not None:
            return {"status": "failed", "reason": pending.error, "request_id": request_id}, 502
        return pending.result or {"status": "failed", "reason": "missing scheduler result"}, 200

    def _build_planner(self) -> PlacementPlanner:
        server_states = self._collect_server_states()
        return PlacementPlanner(
            servers=list(self.servers.values()),
            services=self.service_catalog,
            workload_forecasts=self.workload_forecasts,
            window_seconds=self.window_seconds,
            priority_services=self.priority_services,
            server_states=server_states,
            operator_configs=self.operator_configs,
        )

    def _collect_server_states(self) -> List[ServerState]:
        states: List[ServerState] = []
        for name in self.servers:
            psm = self.server_service_metrics.get(name, {})
            per_service_ql: Dict[str, int] = {}
            for svc, info in psm.items():
                per_service_ql[svc] = int(info.get("queued", 0)) if isinstance(info, dict) else 0
            states.append(ServerState(
                name=name,
                credit_pool=self.server_credits.get(name, 0),
                violation_estimate=self.server_violation_estimates.get(name, 0.0),
                per_service_queue_len=per_service_ql,
            ))
        return states

    def _load_initial_plan(self) -> PlacementPlan:
        if self.placement_mode == "static":
            return self._load_static_plan(push_plan=False)
        return self._recompute_plan(push_plan=False)

    def _validate_static_plan(self, plan: PlacementPlan) -> None:
        unknown_services = sorted({choice.service for choice in plan.placements if choice.service not in self.service_catalog})
        unknown_servers = sorted(
            {
                server_name
                for choice in plan.placements
                for server_name in choice.server_ids
                if server_name not in self.servers
            }
        )
        if unknown_services:
            raise ValueError(f"unknown services in static placement plan: {unknown_services}")
        if unknown_servers:
            raise ValueError(f"unknown servers in static placement plan: {unknown_servers}")

        objective_value = self._build_planner().objective(plan.placements)
        if objective_value == float("-inf"):
            raise ValueError("static placement plan is infeasible for the configured resources")

    def _load_static_plan(self, push_plan: bool) -> PlacementPlan:
        if self.placement_plan_path is None:
            raise ValueError("placement.mode is static but no placement.plan_file is configured")
        payload = load_json_file(self.placement_plan_path)
        plan = PlacementPlan.from_dict(payload)
        self._validate_static_plan(plan)

        objective_value = self._build_planner().objective(plan.placements)
        loaded_plan = PlacementPlan(
            placements=plan.placements,
            objective_value=objective_value,
            window_seconds=plan.window_seconds,
        )
        if push_plan:
            self._push_plan_to_servers(loaded_plan)
        return loaded_plan

    def _recompute_plan(self, push_plan: bool) -> PlacementPlan:
        planner = self._build_planner()
        plan = planner.build_plan()
        # Resolve operator configs for all placements
        allocator = TaskResourceAllocator(
            servers=self.servers,
            services=self.service_catalog,
            forecasts=self.workload_forecasts,
        )
        self.operator_configs = allocator.resolve_all(plan.placements)
        if push_plan:
            self._push_plan_to_servers(plan)
        return plan

    def _invoke_server_rpc(
        self,
        server_name: str,
        method_name: str,
        payload: Optional[Dict[str, Any]],
        timeout: float,
    ) -> Tuple[int, Dict[str, Any]]:
        stub = self.server_stubs[server_name]
        method = getattr(stub, method_name)
        request = farfly_pb2.Empty() if payload is None else make_json_request(payload)
        reply = method(request, timeout=timeout)
        return parse_json_reply(reply)

    def _push_plan_to_servers(self, plan: PlacementPlan) -> None:
        placement_map = plan.server_service_map()
        version = str(time.time())
        for server_name in self.servers:
            svc_list = placement_map.get(server_name, [])
            # Include per-service operator configs for this server
            ops_for_server: Dict[str, Dict[str, Any]] = {}
            for svc in svc_list:
                key = (svc, server_name)
                op = self.operator_configs.get(key)
                if op is not None:
                    ops_for_server[svc] = op.to_dict()
            payload = {
                "version": version,
                "services": svc_list,
                "operator_configs": ops_for_server,
            }
            try:
                status_code, _ = self._invoke_server_rpc(server_name, "UpdatePlacement", payload, timeout=2.0)
                if status_code >= 400:
                    continue
            except grpc.RpcError:
                continue

    def _dispatch_loop(self) -> None:
        self._push_plan_to_servers(self.plan)
        while self.running:
            with self.condition:
                while self.running and not any(not aqm.is_empty for aqm in self.service_aqm.values()):
                    self.condition.wait(timeout=0.5)
                if not self.running:
                    return

                # Pick the service with the earliest deadline across all per-service heaps
                best_svc: Optional[str] = None
                best_deadline = float("inf")
                for svc_name, aqm in self.service_aqm.items():
                    if not aqm.is_empty and aqm.peek_deadline < best_deadline:
                        best_deadline = aqm.peek_deadline
                        best_svc = svc_name

                if best_svc is None:
                    continue

                batch = self._build_service_batch_locked(best_svc)

            if batch is None:
                time.sleep(0.01)
                continue

            server_name = self._choose_server(batch)
            if server_name is None:
                self._fail_batch(batch, "no server placement available")
                continue
            self.dispatch_executor.submit(self._dispatch_batch, server_name, batch)

    def _pop_matching_from_heap(
        self,
        heap: List[Tuple[float, int, PendingRequest]],
        service_name: str,
        max_items: int,
        flow_id: Optional[str] = None,
    ) -> List[PendingRequest]:
        matched: List[PendingRequest] = []
        remaining: List[Tuple[float, int, PendingRequest]] = []
        while heap:
            deadline_at, sequence_id, pending = heapq.heappop(heap)
            if (
                len(matched) < max_items
                and pending.service == service_name
                and (flow_id is None or pending.flow_id == flow_id)
            ):
                matched.append(pending)
            else:
                remaining.append((deadline_at, sequence_id, pending))
        for item in remaining:
            heapq.heappush(heap, item)
        return matched

    def _build_service_batch_locked(self, service_name: str) -> Optional[List[PendingRequest]]:
        aqm = self.service_aqm.get(service_name)
        if aqm is None or aqm.is_empty:
            return None

        profile = self.service_catalog[service_name]
        _, _, head = heapq.heappop(aqm.heap)
        batch: List[PendingRequest] = [head]

        if profile.is_frequency:
            same_flow_limit = max(0, profile.max_inter_frame_count - 1)
            if same_flow_limit:
                batch.extend(self._pop_matching_from_heap(
                    aqm.heap, service_name, same_flow_limit, flow_id=head.flow_id,
                ))
            remaining_capacity = max(0, profile.batch_size - len(batch))
            if remaining_capacity:
                batch.extend(self._pop_matching_from_heap(aqm.heap, service_name, remaining_capacity))
        else:
            remaining_capacity = max(0, profile.batch_size - 1)
            if remaining_capacity:
                batch.extend(self._pop_matching_from_heap(aqm.heap, service_name, remaining_capacity))

        return batch

    def _choose_server(self, batch: List[PendingRequest]) -> Optional[str]:
        head = batch[0]
        placements = self.plan.placements_for_service(head.service)
        if not placements:
            return None

        candidates = list(dict.fromkeys(choice.primary_server for choice in placements))
        offset = self.service_server_offsets.get(head.service, 0)
        if candidates:
            rotation = offset % len(candidates)
            candidates = candidates[rotation:] + candidates[:rotation]
            self.service_server_offsets[head.service] = offset + 1

        # ---- Baseline dispatch policies ----
        if self.dispatch_policy == "round_robin":
            return candidates[0]  # already rotated above

        if self.dispatch_policy == "random":
            return _random.choice(candidates)

        if self.dispatch_policy == "least_violation":
            # Pick server with lowest violation estimate (ignores credit for freq)
            return min(
                candidates,
                key=lambda s: self.server_violation_estimates.get(s, 0.0),
            )

        # ---- Default: farfly policy (rotation + credit / violation awareness) ----
        # Start with rotated server (even distribution) then apply intelligence
        rotated = candidates[0]  # already rotated above

        if head.task_type == "frequency":
            # Prefer servers that have credits, but keep rotation for balance
            credit_ready = [s for s in candidates if self.server_credits.get(s, 0) >= len(batch)]
            if credit_ready:
                if rotated in credit_ready:
                    return rotated  # rotated has credits — best of both worlds
                # Rotated doesn't have credits; use first credit-ready (still rotates via offset)
                return credit_ready[0]
            # No credits anywhere — fall back to rotation
            return rotated

        # Latency tasks: rotation + violation guardrail
        best = min(candidates, key=lambda s: self.server_violation_estimates.get(s, 0.0))
        best_viol = self.server_violation_estimates.get(best, 0.0)
        rotated_viol = self.server_violation_estimates.get(rotated, 0.0)
        # If rotated server is close enough to best, keep rotation (avoids herding)
        if rotated_viol - best_viol <= 0.15:
            return rotated
        return best

    def _dispatch_batch(self, server_name: str, batch: List[PendingRequest]) -> None:
        service_name = batch[0].service
        task_type = batch[0].task_type
        profile = self.service_catalog[service_name]

        payload = {
            "service": service_name,
            "task_type": task_type,
            "timeout_seconds": self.dispatch_timeout_seconds,
            "requests": [
                {
                    "request_id": pending.request_id,
                    "payload": pending.payload,
                    "created_at": pending.created_at,
                    "deadline_at": pending.deadline_at,
                    "flow_id": pending.flow_id,
                    "frame_id": pending.frame_id,
                }
                for pending in batch
            ],
        }

        try:
            status_code, body = self._invoke_server_rpc(
                server_name,
                "ExecuteBatch",
                payload,
                timeout=self.dispatch_timeout_seconds,
            )
            if status_code >= 400:
                self._fail_batch(batch, f"dispatch to {server_name} failed with status {status_code}: {body}")
                return
        except grpc.RpcError as exc:
            self._fail_batch(batch, f"dispatch to {server_name} failed: {exc}")
            return

        request_results = {item["request_id"]: item for item in body.get("request_results", [])}
        violation_fraction = float(body.get("violation_fraction", 0.0))
        # Credits are now independent: do NOT extract available_credits from response.
        # The credit assigner loop and ReportCredits RPC handle credit updates.

        with self.condition:
            # Still update violation estimate (supplementary to async)
            self.server_violation_estimates[server_name] = (
                (1.0 - self.violation_alpha) * self.server_violation_estimates.get(server_name, 0.0)
                + self.violation_alpha * violation_fraction
            )
            self.server_last_seen[server_name] = time.time()
            # Update AQM delay tracking
            aqm = self.service_aqm.get(service_name)
            if aqm is not None:
                for pending in batch:
                    aqm.update_delay((time.time() - pending.created_at) * 1000.0)
                aqm.total_dispatched += len(batch)

        for pending in batch:
            item = request_results.get(pending.request_id)
            if item is None:
                pending.error = f"missing result for {pending.request_id}"
                pending.event.set()
                continue
            pending.result = {
                "status": "success",
                "request_id": pending.request_id,
                "service": service_name,
                "task_type": task_type,
                "scheduled_server": server_name,
                "result": item.get("result", {}),
                "metrics": {
                    "violated_slo": item.get("violated_slo", False),
                    "queue_delay_ms": item.get("queue_delay_ms", 0.0),
                    "processing_time_ms": item.get("processing_time_ms", 0.0),
                    "server_credits": self.server_credits.get(server_name, 0),
                    "violation_fraction": violation_fraction,
                },
            }
            with self.condition:
                self.total_completed += 1
                if item.get("violated_slo", False):
                    self.total_violations += 1
            pending.event.set()

        if task_type == "frequency":
            with self.condition:
                self.server_credits[server_name] = max(-profile.max_credit_pool, self.server_credits[server_name] - len(batch))

    def _fail_batch(self, batch: List[PendingRequest], reason: str) -> None:
        with self.condition:
            self.total_failed += len(batch)
        for pending in batch:
            pending.error = reason
            pending.event.set()

    # ---- independent credit assigner (polls servers periodically) ----

    def _credit_assigner_loop(self) -> None:
        while self.running:
            time.sleep(self.credit_poll_interval)
            for server_name in list(self.servers.keys()):
                try:
                    status_code, body = self._invoke_server_rpc(server_name, "Metrics", None, timeout=2.0)
                    if status_code >= 400:
                        continue
                    with self.condition:
                        self.server_credits[server_name] = int(
                            body.get("credit_pool", self.server_credits.get(server_name, 0))
                        )
                        total_req = int(body.get("total_requests", 0))
                        violation_req = int(body.get("violation_requests", 0))
                        if total_req > 0:
                            v_frac = violation_req / total_req
                            self.server_violation_estimates[server_name] = (
                                (1 - self.violation_alpha) * self.server_violation_estimates.get(server_name, 0.0)
                                + self.violation_alpha * v_frac
                            )
                        self.server_last_seen[server_name] = time.time()
                        psm = body.get("per_service_metrics")
                        if psm:
                            self.server_service_metrics[server_name] = psm
                except grpc.RpcError:
                    pass

    # ---- server-push credit path (ReportCredits RPC) ----

    def report_credits_payload(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        server_name = str(payload.get("server", ""))
        if server_name not in self.servers:
            return {"status": "ignored", "reason": "unknown server"}, 404
        with self.condition:
            self.server_credits[server_name] = int(
                payload.get("credit_pool", self.server_credits.get(server_name, 0))
            )
            vf = float(payload.get("violation_fraction", 0.0))
            self.server_violation_estimates[server_name] = (
                (1.0 - self.violation_alpha) * self.server_violation_estimates.get(server_name, 0.0)
                + self.violation_alpha * vf
            )
            self.server_last_seen[server_name] = time.time()
            psm = payload.get("per_service_metrics")
            if psm:
                self.server_service_metrics[server_name] = psm
        return {"status": "ok"}, 200

    def run(self) -> None:
        grpc_server = grpc.server(ThreadPoolExecutor(max_workers=max(1, self.rpc_workers)))
        farfly_pb2_grpc.add_FarflySchedulerServiceServicer_to_server(_SchedulerServicer(self), grpc_server)
        grpc_server.add_insecure_port(f"{self.ip}:{self.port}")
        grpc_server.start()
        grpc_server.wait_for_termination()


class _SchedulerServicer(farfly_pb2_grpc.FarflySchedulerServiceServicer):
    def __init__(self, scheduler: FarflyScheduler) -> None:
        self.scheduler = scheduler

    def Health(self, request, context):
        return make_json_reply(self.scheduler.health_payload())

    def Metrics(self, request, context):
        return make_json_reply(self.scheduler.metrics_payload())

    def Plan(self, request, context):
        return make_json_reply(self.scheduler.plan_payload())

    def RecomputePlan(self, request, context):
        body, status_code = self.scheduler.recompute_plan_payload()
        return make_json_reply(body, status_code)

    def ReloadPlan(self, request, context):
        body, status_code = self.scheduler.reload_plan_payload()
        return make_json_reply(body, status_code)

    def Submit(self, request, context):
        body, status_code = self.scheduler.submit_payload(parse_json_request(request))
        return make_json_reply(body, status_code)

    def ReportCredits(self, request, context):
        body, status_code = self.scheduler.report_credits_payload(parse_json_request(request))
        return make_json_reply(body, status_code)


def start_scheduler(config_file: str) -> None:
    scheduler = FarflyScheduler(config_file)
    scheduler.run()