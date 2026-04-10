from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


def load_json_file(file_path: Path) -> Dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_relative_path(config_path: Path, target: str) -> Path:
    candidate = Path(target)
    if candidate.is_absolute():
        return candidate
    return (config_path.parent / candidate).resolve()


@dataclass(frozen=True)
class ServerResource:
    name: str
    ip: str
    port: int
    compute_capacity: float
    vram_capacity_gb: float
    queue_capacity: int
    gpu_count: int = 1
    initial_credit_pool: int = 8

    @classmethod
    def from_dict(cls, item: Mapping[str, Any]) -> "ServerResource":
        resource = dict(item.get("resource", {}))
        return cls(
            name=str(item["name"]),
            ip=str(item["ip"]),
            port=int(item["port"]),
            compute_capacity=float(resource.get("compute_capacity", 1.0)),
            vram_capacity_gb=float(resource.get("vram_capacity_gb", 16.0)),
            queue_capacity=int(resource.get("queue_capacity", 32)),
            gpu_count=int(resource.get("gpu_count", 1)),
            initial_credit_pool=int(resource.get("initial_credit_pool", 8)),
        )


@dataclass(frozen=True)
class ServiceProfile:
    name: str
    task_type: str
    model_family: str
    workspace_model: str
    batch_size: int
    mt_degree: int = 1
    mp_degree: int = 1
    allow_cross_server_mp: bool = False
    max_inter_frame_count: int = 1
    fps_gpu: float = 0.0
    fps_slo: float = 0.0
    base_service_time_ms: float = 100.0
    compute_cost: float = 0.1
    vram_cost_gb: float = 1.0
    timeout_ms: int = 1000
    queue_target_ms: float = 100.0
    additive_credit_step: int = 1
    multiplicative_beta: float = 0.2
    max_credit_pool: int = 32
    priority_weight: float = 1.0

    @classmethod
    def from_dict(cls, item: Mapping[str, Any]) -> "ServiceProfile":
        return cls(
            name=str(item["name"]),
            task_type=str(item["task_type"]),
            model_family=str(item.get("model_family", "generic")),
            workspace_model=str(item.get("workspace_model", "")),
            batch_size=int(item.get("batch_size", 1)),
            mt_degree=int(item.get("mt_degree", 1)),
            mp_degree=int(item.get("mp_degree", 1)),
            allow_cross_server_mp=bool(item.get("allow_cross_server_mp", False)),
            max_inter_frame_count=int(item.get("max_inter_frame_count", 1)),
            fps_gpu=float(item.get("fps_gpu", 0.0)),
            fps_slo=float(item.get("fps_slo", 0.0)),
            base_service_time_ms=float(item.get("base_service_time_ms", 100.0)),
            compute_cost=float(item.get("compute_cost", 0.1)),
            vram_cost_gb=float(item.get("vram_cost_gb", 1.0)),
            timeout_ms=int(item.get("timeout_ms", 1000)),
            queue_target_ms=float(item.get("queue_target_ms", 100.0)),
            additive_credit_step=int(item.get("additive_credit_step", 1)),
            multiplicative_beta=float(item.get("multiplicative_beta", 0.2)),
            max_credit_pool=int(item.get("max_credit_pool", 32)),
            priority_weight=float(item.get("priority_weight", 1.0)),
        )

    @property
    def is_frequency(self) -> bool:
        return self.task_type == "frequency"

    @property
    def dp_groups(self) -> int:
        if not self.is_frequency or self.fps_gpu <= 0 or self.fps_slo <= 0:
            return 1
        return max(1, math.ceil(self.fps_slo / self.fps_gpu))

    @property
    def multiframe_batch_width(self) -> int:
        return max(1, self.max_inter_frame_count)

    @property
    def multiframe_task_count(self) -> int:
        return max(1, self.batch_size // self.multiframe_batch_width)

    def estimated_service_time_seconds(self, batch_items: int) -> float:
        item_count = max(1, batch_items)
        batch_gain = max(1.0, min(self.batch_size, item_count) ** 0.55)
        mt_gain = max(1, self.mt_degree)
        mp_gain = 1.0 + 0.75 * (max(1, self.mp_degree) - 1)
        mf_gain = 1.0
        if self.is_frequency:
            mf_gain = 1.0 + 0.25 * (min(self.multiframe_batch_width, item_count) - 1)
        throughput_gain = batch_gain * mt_gain * mp_gain * mf_gain
        return (self.base_service_time_ms / 1000.0) * item_count / throughput_gain

    def throughput_per_second(self) -> float:
        batch_items = self.multiframe_task_count if self.is_frequency else self.batch_size
        return batch_items / max(self.estimated_service_time_seconds(batch_items), 1e-6)


@dataclass(frozen=True)
class WorkloadForecast:
    service: str
    task_type: str
    arrival_rate: float = 0.0
    flow_count: int = 0
    fps_slo: float = 0.0
    weight: float = 1.0

    @classmethod
    def from_dict(cls, item: Mapping[str, Any]) -> "WorkloadForecast":
        return cls(
            service=str(item["service"]),
            task_type=str(item["task_type"]),
            arrival_rate=float(item.get("arrival_rate", 0.0)),
            flow_count=int(item.get("flow_count", 0)),
            fps_slo=float(item.get("fps_slo", 0.0)),
            weight=float(item.get("weight", 1.0)),
        )

    def demand(self, window_seconds: float, profile: ServiceProfile) -> float:
        if self.task_type == "frequency":
            fps_value = self.fps_slo or profile.fps_slo or profile.fps_gpu or 1.0
            return max(1, self.flow_count) * fps_value * window_seconds
        return max(0.0, self.arrival_rate) * window_seconds


@dataclass
class ServerState:
    """Real-time server processing state for state-based placement."""
    name: str
    queued_batches: int = 0
    active_batches: int = 0
    credit_pool: int = 0
    violation_estimate: float = 0.0
    average_queue_delay_ms: float = 0.0
    total_requests: int = 0
    per_service_queue_len: Dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, item: Mapping[str, Any]) -> "ServerState":
        return cls(
            name=str(item.get("name", item.get("server", ""))),
            queued_batches=int(item.get("queued_batches", 0)),
            active_batches=int(item.get("active_batches", 0)),
            credit_pool=int(item.get("credit_pool", 0)),
            violation_estimate=float(item.get("violation_estimate", 0.0)),
            average_queue_delay_ms=float(item.get("average_queue_delay_ms", 0.0)),
            total_requests=int(item.get("total_requests", 0)),
            per_service_queue_len=dict(item.get("per_service_queue_len", {})),
        )


@dataclass(frozen=True)
class PlacementChoice:
    service: str
    server_ids: Tuple[str, ...]
    stage: str

    @classmethod
    def from_dict(cls, item: Mapping[str, Any]) -> "PlacementChoice":
        raw_server_ids = item.get("server_ids")
        if raw_server_ids:
            server_ids = tuple(str(server_name) for server_name in raw_server_ids)
        else:
            primary_server = item.get("primary_server")
            if primary_server is None:
                raise ValueError("placement choice requires server_ids or primary_server")
            server_ids = (str(primary_server),)
        return cls(
            service=str(item["service"]),
            server_ids=server_ids,
            stage=str(item.get("stage", "static")),
        )

    @property
    def primary_server(self) -> str:
        return self.server_ids[0]

    @property
    def spread(self) -> int:
        return len(self.server_ids)


@dataclass
class PlacementPlan:
    placements: List[PlacementChoice] = field(default_factory=list)
    objective_value: float = 0.0
    window_seconds: float = 60.0

    @classmethod
    def from_dict(cls, item: Mapping[str, Any]) -> "PlacementPlan":
        placements = [PlacementChoice.from_dict(choice) for choice in item.get("placements", [])]
        return cls(
            placements=placements,
            objective_value=float(item.get("objective_value", 0.0)),
            window_seconds=float(item.get("window_seconds", 60.0)),
        )

    def placements_for_service(self, service_name: str) -> List[PlacementChoice]:
        return [choice for choice in self.placements if choice.service == service_name]

    def placements_for_server(self, server_name: str) -> List[PlacementChoice]:
        return [choice for choice in self.placements if server_name in choice.server_ids]

    def server_service_map(self) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for choice in self.placements:
            for server_name in choice.server_ids:
                mapping.setdefault(server_name, []).append(choice.service)
        for services in mapping.values():
            services.sort()
        return mapping

    def to_dict(self) -> Dict[str, Any]:
        return {
            "objective_value": self.objective_value,
            "window_seconds": self.window_seconds,
            "placements": [
                {
                    "service": choice.service,
                    "server_ids": list(choice.server_ids),
                    "primary_server": choice.primary_server,
                    "stage": choice.stage,
                }
                for choice in self.placements
            ],
            "server_service_map": self.server_service_map(),
        }


def load_service_catalog(config_path: Path, config_data: Mapping[str, Any]) -> Dict[str, ServiceProfile]:
    service_file = config_data.get("service_catalog_file")
    if service_file:
        payload = load_json_file(resolve_relative_path(config_path, str(service_file)))
        items = payload.get("services", [])
    else:
        items = config_data.get("services", [])
    catalog = {profile.name: profile for profile in (ServiceProfile.from_dict(item) for item in items)}
    return catalog


def load_workload_forecast(config_path: Path, config_data: Mapping[str, Any]) -> List[WorkloadForecast]:
    forecast_file = config_data.get("workload_forecast_file")
    if forecast_file:
        payload = load_json_file(resolve_relative_path(config_path, str(forecast_file)))
        items = payload.get("forecasts", [])
    else:
        items = config_data.get("workload_forecasts", [])
    return [WorkloadForecast.from_dict(item) for item in items]


def server_url(server: ServerResource, path: str) -> str:
    clean_path = path if path.startswith("/") else f"/{path}"
    return f"http://{server.ip}:{server.port}{clean_path}"


class PlacementPlanner:
    def __init__(
        self,
        servers: Sequence[ServerResource],
        services: Mapping[str, ServiceProfile],
        workload_forecasts: Sequence[WorkloadForecast],
        window_seconds: float,
        priority_services: Optional[Sequence[str]] = None,
        server_states: Optional[Sequence[ServerState]] = None,
        operator_configs: Optional[Mapping[tuple, Any]] = None,
    ) -> None:
        self.servers = list(servers)
        self.services = dict(services)
        self.workload_forecasts = list(workload_forecasts)
        self.window_seconds = float(window_seconds)
        self.priority_services = set(priority_services or [])
        self.server_index = {server.name: server for server in self.servers}
        self.server_states: Dict[str, ServerState] = {
            s.name: s for s in (server_states or [])
        }
        self.operator_configs: Dict[tuple, Any] = dict(operator_configs or {})

    def _resource_usage(self, choice: PlacementChoice) -> Dict[str, Tuple[float, float]]:
        profile = self.services[choice.service]
        usage: Dict[str, Tuple[float, float]] = {}
        if len(choice.server_ids) == 1:
            usage[choice.server_ids[0]] = (profile.compute_cost, profile.vram_cost_gb)
            return usage
        share = len(choice.server_ids)
        for server_name in choice.server_ids:
            usage[server_name] = (profile.compute_cost / share, profile.vram_cost_gb / share)
        return usage

    def _is_feasible(self, placements: Sequence[PlacementChoice]) -> bool:
        used_compute = {server.name: 0.0 for server in self.servers}
        used_vram = {server.name: 0.0 for server in self.servers}
        for choice in placements:
            profile = self.services[choice.service]
            if len(choice.server_ids) > 1 and not profile.allow_cross_server_mp:
                return False
            if len(choice.server_ids) < profile.mp_degree and profile.mp_degree > 1:
                return False
            for server_name, (compute_cost, vram_cost) in self._resource_usage(choice).items():
                used_compute[server_name] += compute_cost
                used_vram[server_name] += vram_cost
                server = self.server_index[server_name]
                if used_compute[server_name] > server.compute_capacity + 1e-9:
                    return False
                if used_vram[server_name] > server.vram_capacity_gb + 1e-9:
                    return False
            # GPU feasibility: per-placement check (GPUs are time-shared across services)
            op_key = (choice.service, choice.primary_server)
            op = self.operator_configs.get(op_key)
            if op is not None:
                gpu_need_per_server = max(1, op.gpu_demand // max(1, len(choice.server_ids)))
            else:
                gpu_need_per_server = max(1, profile.mp_degree // max(1, len(choice.server_ids)))
            for sn in choice.server_ids:
                if gpu_need_per_server > self.server_index[sn].gpu_count:
                    return False
        return True

    def _placement_supply(self, choice: PlacementChoice) -> float:
        profile = self.services[choice.service]

        # Use operator config throughput if available
        op_key = (choice.service, choice.primary_server)
        op = self.operator_configs.get(op_key)
        if op is not None:
            supply = op.effective_throughput(profile.base_service_time_ms, profile.is_frequency)
        else:
            supply = profile.throughput_per_second()
            if len(choice.server_ids) > 1:
                supply *= 1.0 + 0.15 * (len(choice.server_ids) - 1)

        # State-based discount: busy servers yield less effective supply
        state = self.server_states.get(choice.primary_server)
        if state is not None:
            server = self.server_index.get(choice.primary_server)
            if server is not None:
                utilization = (
                    (state.active_batches + state.queued_batches)
                    / max(server.queue_capacity, 1)
                )
                supply *= max(0.3, 1.0 - 0.5 * utilization)

        return supply

    def objective(self, placements: Sequence[PlacementChoice]) -> float:
        if not self._is_feasible(placements):
            return float("-inf")

        supply_per_service: Dict[str, float] = {service_name: 0.0 for service_name in self.services}
        for choice in placements:
            supply_per_service[choice.service] += self._placement_supply(choice)

        # Submodular value: min(demand, supply) is concave in supply
        total_value = 0.0
        for forecast in self.workload_forecasts:
            profile = self.services[forecast.service]
            demand = forecast.demand(self.window_seconds, profile)
            supply = supply_per_service[forecast.service] * self.window_seconds
            served = min(demand, supply)
            total_value += served * forecast.weight * profile.priority_weight

        # Resource cost penalty (submodular diminishing returns)
        placement_penalty = sum(self.services[choice.service].vram_cost_gb * 0.01 for choice in placements)

        # State-based violation penalty: avoid servers with high violations
        violation_penalty = 0.0
        for choice in placements:
            state = self.server_states.get(choice.primary_server)
            if state is not None and state.violation_estimate > 0.0:
                violation_penalty += (
                    state.violation_estimate
                    * self.services[choice.service].priority_weight
                    * 50.0
                )

        return total_value - placement_penalty - violation_penalty

    def _candidate_pool(self) -> List[PlacementChoice]:
        candidates: List[PlacementChoice] = []
        for service_name, profile in self.services.items():
            candidates.extend(
                PlacementChoice(service=service_name, server_ids=(server.name,), stage="single")
                for server in self.servers
            )
            if profile.mp_degree > 1 and profile.allow_cross_server_mp:
                for group in combinations(self.servers, min(profile.mp_degree, len(self.servers))):
                    candidates.append(
                        PlacementChoice(
                            service=service_name,
                            server_ids=tuple(server.name for server in group),
                            stage="group",
                        )
                    )
        deduplicated = {(choice.service, choice.server_ids): choice for choice in candidates}
        return list(deduplicated.values())

    def _greedy_stage(
        self,
        seed: Sequence[PlacementChoice],
        candidates: Sequence[PlacementChoice],
        allow_equal: bool,
    ) -> List[PlacementChoice]:
        current = list(seed)
        current_score = self.objective(current)
        remaining = [candidate for candidate in candidates if candidate not in current]

        while remaining:
            best_score = current_score
            best_candidates: List[PlacementChoice] = []
            for candidate in remaining:
                trial = current + [candidate]
                trial_score = self.objective(trial)
                if trial_score == float("-inf"):
                    continue
                if trial_score > best_score + 1e-9:
                    best_score = trial_score
                    best_candidates = [candidate]
                elif allow_equal and abs(trial_score - best_score) <= 1e-9 and trial_score >= current_score - 1e-9:
                    best_candidates.append(candidate)

            if not best_candidates:
                break

            chosen = sorted(best_candidates, key=lambda item: (item.service, item.server_ids))[0]
            current.append(chosen)
            current_score = best_score
            remaining.remove(chosen)

        return current

    def build_plan(self) -> PlacementPlan:
        all_candidates = self._candidate_pool()
        priority_candidates = [candidate for candidate in all_candidates if candidate.service in self.priority_services]
        colocated_candidates = [candidate for candidate in all_candidates if len(candidate.server_ids) == 1]
        cross_server_candidates = [candidate for candidate in all_candidates if len(candidate.server_ids) > 1]

        stage_one = self._greedy_stage([], priority_candidates, allow_equal=True)
        stage_two = self._greedy_stage(stage_one, colocated_candidates, allow_equal=False)
        stage_three = self._greedy_stage(stage_two, cross_server_candidates, allow_equal=False)

        final_score = self.objective(stage_three)
        final_placements = stage_three if final_score != float("-inf") else stage_two
        final_score = self.objective(final_placements)
        return PlacementPlan(placements=final_placements, objective_value=final_score, window_seconds=self.window_seconds)