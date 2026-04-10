from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
import threading
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

import grpc

import farfly_pb2
import farfly_pb2_grpc
from farfly_core import ServiceProfile, load_json_file, load_service_catalog
from farfly_operators import OperatorConfig
from farfly_rpc import grpc_target, make_json_reply, make_json_request, parse_json_request


@dataclass
class QueueEnvelope:
    service: str
    task_type: str
    requests: List[Dict[str, Any]]
    created_at: float = field(default_factory=time.time)
    done_event: threading.Event = field(default_factory=threading.Event)
    result: Optional[Dict[str, Any]] = None

    @property
    def earliest_deadline(self) -> float:
        if not self.requests:
            return self.created_at
        deadlines = [float(item.get("deadline_at", self.created_at)) for item in self.requests]
        return min(deadlines)


@dataclass
class ServiceServerQueue:
    """Per-service AQM state on the server side."""
    service_name: str
    latency: Deque[QueueEnvelope] = field(default_factory=deque)
    frequency: Deque[QueueEnvelope] = field(default_factory=deque)
    preempt: Deque[QueueEnvelope] = field(default_factory=deque)
    ewma_delay_ms: float = 0.0
    total_enqueued: int = 0
    total_processed: int = 0
    total_violations: int = 0
    _alpha: float = 0.15

    @property
    def total_queued(self) -> int:
        return len(self.latency) + len(self.frequency) + len(self.preempt)

    @property
    def has_items(self) -> bool:
        return bool(self.latency or self.frequency or self.preempt)

    def update_delay(self, delay_ms: float) -> None:
        self.ewma_delay_ms = (1 - self._alpha) * self.ewma_delay_ms + self._alpha * delay_ms


class FarflyServer:
    def __init__(self, config_file: str) -> None:
        self.config_path = Path(config_file).resolve()
        self.config = load_json_file(self.config_path)

        self.name = str(self.config.get("name", f"{self.config['self']['exposed_ip']}:{self.config['self']['port']}"))
        self.ip = str(self.config["self"]["ip"])
        self.port = int(self.config["self"]["port"])
        self.exposed_ip = str(self.config["self"]["exposed_ip"])

        resource = dict(self.config.get("resource", {}))
        self.queue_capacity = int(resource.get("queue_capacity", 32))
        self.max_concurrent = int(self.config.get("max_concurrent_tasks", 1))
        self.credit_pool = int(resource.get("initial_credit_pool", 8))
        self.max_credit_pool = int(resource.get("max_credit_pool", max(self.credit_pool, 8)))
        self.rpc_workers = int(self.config.get("rpc_workers", max(8, self.max_concurrent * 2)))

        self.service_catalog = load_service_catalog(self.config_path, self.config)
        self.loaded_services = set(self.service_catalog)
        self.plan_version = "bootstrap"

        # Per-service queues (AQM state)
        self.service_queues: Dict[str, ServiceServerQueue] = {}
        # Operator configs pushed by scheduler
        self.operator_configs: Dict[str, OperatorConfig] = {}

        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.running = True

        self.total_requests = 0
        self.total_batches = 0
        self.success_requests = 0
        self.violation_requests = 0
        self.total_queue_delay_ms = 0.0
        self.active_batches = 0

        # Scheduler channel for async credit reporting (independent feedback path)
        scheduler_cfg = self.config.get("scheduler", {})
        self._scheduler_ip = str(scheduler_cfg.get("ip", ""))
        self._scheduler_port = int(scheduler_cfg.get("port", 0))
        self._scheduler_stub: Optional[farfly_pb2_grpc.FarflySchedulerServiceStub] = None
        if self._scheduler_ip and self._scheduler_port:
            channel = grpc.insecure_channel(grpc_target(self._scheduler_ip, self._scheduler_port))
            self._scheduler_stub = farfly_pb2_grpc.FarflySchedulerServiceStub(channel)

        self._start_workers()

    def health_payload(self) -> Dict[str, Any]:
        return {"status": "ok", "server": self.name, "plan_version": self.plan_version}

    def _get_service_queue(self, service_name: str) -> ServiceServerQueue:
        sq = self.service_queues.get(service_name)
        if sq is None:
            sq = ServiceServerQueue(service_name=service_name)
            self.service_queues[service_name] = sq
        return sq

    def _total_queued(self) -> int:
        return sum(sq.total_queued for sq in self.service_queues.values())

    def _any_has_items(self) -> bool:
        return any(sq.has_items for sq in self.service_queues.values())

    def metrics_payload(self) -> Dict[str, Any]:
        with self.lock:
            avg_queue_delay_ms = self.total_queue_delay_ms / max(1, self.total_batches)
            per_svc: Dict[str, Dict[str, Any]] = {}
            for sn, sq in self.service_queues.items():
                per_svc[sn] = {
                    "queued": sq.total_queued,
                    "ewma_delay_ms": sq.ewma_delay_ms,
                    "total_enqueued": sq.total_enqueued,
                    "total_processed": sq.total_processed,
                    "total_violations": sq.total_violations,
                }
            return {
                "server": self.name,
                "plan_version": self.plan_version,
                "loaded_services": sorted(self.loaded_services),
                "credit_pool": self.credit_pool,
                "active_batches": self.active_batches,
                "queued_batches": self._total_queued(),
                "total_requests": self.total_requests,
                "total_batches": self.total_batches,
                "success_requests": self.success_requests,
                "violation_requests": self.violation_requests,
                "average_queue_delay_ms": avg_queue_delay_ms,
                "per_service_metrics": per_svc,
            }

    def update_placement_payload(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        services = payload.get("services") or []
        ops_raw = payload.get("operator_configs") or {}
        with self.lock:
            self.loaded_services = set(str(service) for service in services)
            self.plan_version = str(payload.get("version", time.time()))
            for svc_name, op_dict in ops_raw.items():
                self.operator_configs[svc_name] = OperatorConfig(**op_dict)
        return {"status": "updated", "server": self.name, "services": sorted(self.loaded_services)}, 200

    def execute_batch_payload(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        service_name = str(payload.get("service", ""))
        task_type = str(payload.get("task_type", "latency"))
        requests_payload = list(payload.get("requests", []))

        if service_name not in self.service_catalog:
            return {"status": "failed", "reason": f"unknown service {service_name}"}, 404
        if self.loaded_services and service_name not in self.loaded_services:
            return {"status": "failed", "reason": f"service {service_name} is not placed on {self.name}"}, 409
        if not requests_payload:
            return {"status": "failed", "reason": "empty batch"}, 400

        envelope = QueueEnvelope(service=service_name, task_type=task_type, requests=requests_payload)
        with self.condition:
            if self._total_queued() >= self.queue_capacity:
                return {"status": "failed", "reason": "server queue is full"}, 429
            sq = self._get_service_queue(service_name)
            target_queue = sq.latency if task_type == "latency" else sq.frequency
            target_queue.append(envelope)
            sq.total_enqueued += 1
            self.condition.notify_all()

        timeout_seconds = max(
            float(self.service_catalog[service_name].timeout_ms) / 1000.0 * 2.0,
            float(payload.get("timeout_seconds", 10.0)),
        )
        if not envelope.done_event.wait(timeout=timeout_seconds):
            return {"status": "failed", "reason": "server worker timeout"}, 504
        return envelope.result or {"status": "failed", "reason": "missing worker result"}, 200

    def _start_workers(self) -> None:
        for index in range(max(1, self.max_concurrent)):
            worker = threading.Thread(target=self._worker_loop, args=(index,), daemon=True)
            worker.start()

    def _promote_overdue(self, sq: ServiceServerQueue) -> None:
        now = time.time()
        remaining: Deque[QueueEnvelope] = deque()
        while sq.latency:
            envelope = sq.latency.popleft()
            profile = self.service_catalog[envelope.service]
            estimated_finish = now + self._effective_service_time(profile, len(envelope.requests))
            if estimated_finish >= envelope.earliest_deadline:
                sq.preempt.append(envelope)
            else:
                remaining.append(envelope)
        sq.latency = remaining

    def _next_envelope(self) -> QueueEnvelope:
        # Promote overdue across all services
        for sq in self.service_queues.values():
            self._promote_overdue(sq)

        # Priority: preempt (earliest deadline) > latency (earliest deadline) > frequency
        best: Optional[QueueEnvelope] = None
        best_sq: Optional[ServiceServerQueue] = None
        best_src: Optional[str] = None

        for sq in self.service_queues.values():
            if sq.preempt:
                env = sq.preempt[0]
                if best is None or env.earliest_deadline < best.earliest_deadline:
                    best, best_sq, best_src = env, sq, "preempt"
        if best_sq is not None and best_src is not None:
            getattr(best_sq, best_src).popleft()
            return best

        best, best_sq, best_src = None, None, None
        for sq in self.service_queues.values():
            if sq.latency:
                env = sq.latency[0]
                if best is None or env.earliest_deadline < best.earliest_deadline:
                    best, best_sq, best_src = env, sq, "latency"
        if best_sq is not None and best_src is not None:
            getattr(best_sq, best_src).popleft()
            return best

        for sq in self.service_queues.values():
            if sq.frequency:
                return sq.frequency.popleft()

        raise RuntimeError("_next_envelope called with no items")

    def _effective_service_time(self, profile: ServiceProfile, batch_items: int) -> float:
        op = self.operator_configs.get(profile.name)
        if op is not None:
            return op.estimated_time_s(profile.base_service_time_ms, batch_items, profile.is_frequency)
        return profile.estimated_service_time_seconds(batch_items)

    def _worker_loop(self, worker_index: int) -> None:
        while self.running:
            with self.condition:
                while self.running and not self._any_has_items():
                    self.condition.wait(timeout=0.5)
                if not self.running:
                    return
                envelope = self._next_envelope()
                self.active_batches += 1

            try:
                self._process_envelope(envelope, worker_index)
            finally:
                with self.condition:
                    self.active_batches -= 1

    def _update_credit_pool(self, profile: ServiceProfile, queue_delay_ms: float) -> None:
        if queue_delay_ms < profile.queue_target_ms:
            self.credit_pool = min(profile.max_credit_pool, self.credit_pool + profile.additive_credit_step)
            return

        overload = max(queue_delay_ms - profile.queue_target_ms, 0.0) / max(profile.queue_target_ms, 1.0)
        factor = max(1.0 - profile.multiplicative_beta * overload, 0.5)
        self.credit_pool = max(1, int(self.credit_pool * factor))

    def _process_envelope(self, envelope: QueueEnvelope, worker_index: int) -> None:
        profile = self.service_catalog[envelope.service]
        started_at = time.time()
        queue_delay_ms = (started_at - envelope.created_at) * 1000.0
        processing_seconds = self._effective_service_time(profile, len(envelope.requests))
        time.sleep(processing_seconds)
        finished_at = time.time()

        request_results: List[Dict[str, Any]] = []
        violation_count = 0
        for item in envelope.requests:
            deadline_at = float(item.get("deadline_at", finished_at))
            violated = finished_at > deadline_at
            if violated:
                violation_count += 1
            request_results.append({
                "request_id": item.get("request_id"),
                "service": envelope.service,
                "task_type": envelope.task_type,
                "processed_by": self.name,
                "worker_index": worker_index,
                "queue_delay_ms": queue_delay_ms,
                "processing_time_ms": processing_seconds * 1000.0,
                "finished_at": finished_at,
                "deadline_at": deadline_at,
                "violated_slo": violated,
                "result": {
                    "message": "processed successfully",
                    "service": envelope.service,
                    "payload": item.get("payload", item.get("data", {})),
                },
            })

        violation_fraction = violation_count / max(1, len(envelope.requests))
        with self.lock:
            self.total_batches += 1
            self.total_requests += len(envelope.requests)
            self.success_requests += len(envelope.requests) - violation_count
            self.violation_requests += violation_count
            self.total_queue_delay_ms += queue_delay_ms
            self._update_credit_pool(profile, queue_delay_ms)
            available_credits = self.credit_pool
            # Per-service AQM state
            sq = self._get_service_queue(envelope.service)
            sq.total_processed += 1
            sq.total_violations += violation_count
            sq.update_delay(queue_delay_ms)

        envelope.result = {
            "status": "success",
            "server": self.name,
            "service": envelope.service,
            "task_type": envelope.task_type,
            "queue_delay_ms": queue_delay_ms,
            "processing_time_ms": processing_seconds * 1000.0,
            "violation_fraction": violation_fraction,
            "available_credits": available_credits,
            "request_results": request_results,
        }
        envelope.done_event.set()

        # --- async credit push (independent feedback path) ---
        self._report_credits_async(violation_fraction)

    def _report_credits_async(self, violation_fraction: float) -> None:
        stub = self._scheduler_stub
        if stub is None:
            return
        with self.lock:
            per_svc: Dict[str, Dict[str, Any]] = {}
            for sn, sq in self.service_queues.items():
                per_svc[sn] = {"queued": sq.total_queued, "ewma_delay_ms": sq.ewma_delay_ms}
            payload = {
                "server": self.name,
                "credit_pool": self.credit_pool,
                "violation_fraction": violation_fraction,
                "queued_batches": self._total_queued(),
                "active_batches": self.active_batches,
                "per_service_metrics": per_svc,
            }

        def _push() -> None:
            try:
                stub.ReportCredits(make_json_request(payload), timeout=2.0)
            except Exception:
                pass  # fire-and-forget

        threading.Thread(target=_push, daemon=True).start()

    def run(self) -> None:
        grpc_server = grpc.server(ThreadPoolExecutor(max_workers=max(1, self.rpc_workers)))
        farfly_pb2_grpc.add_ServerServiceServicer_to_server(_ServerServicer(self), grpc_server)
        grpc_server.add_insecure_port(f"{self.ip}:{self.port}")
        grpc_server.start()
        grpc_server.wait_for_termination()


class _ServerServicer(farfly_pb2_grpc.ServerServiceServicer):
    def __init__(self, server_runtime: FarflyServer) -> None:
        self.server_runtime = server_runtime

    def Health(self, request, context):
        return make_json_reply(self.server_runtime.health_payload())

    def Metrics(self, request, context):
        return make_json_reply(self.server_runtime.metrics_payload())

    def UpdatePlacement(self, request, context):
        body, status_code = self.server_runtime.update_placement_payload(parse_json_request(request))
        return make_json_reply(body, status_code)

    def ExecuteBatch(self, request, context):
        body, status_code = self.server_runtime.execute_batch_payload(parse_json_request(request))
        return make_json_reply(body, status_code)


def start_server(config_file: str) -> None:
    server_runtime = FarflyServer(config_file)
    server_runtime.run()