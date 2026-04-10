"""Task-resource allocator operators for FarFly.

Operators
---------
BS  Batch Size      dynamic batch sizing based on queue pressure
MT  Multi-Task      concurrent service execution sharing GPU time-slices
MF  Multi-Frame     frame packing for frequency (video) flows
DP  Data Parallel   model replication across GPUs on the same server
MP  Model Parallel  model split across GPUs (single-machine first, multi-machine via TCP)
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from farfly_core import PlacementChoice, ServerResource, ServiceProfile, WorkloadForecast


# ---------------------------------------------------------------------------
# OperatorConfig – resolved operator settings for one (service, server) pair
# ---------------------------------------------------------------------------

@dataclass
class OperatorConfig:
    service: str
    server: str
    batch_size: int       # BS
    mt_degree: int        # MT
    mf_width: int         # MF  (1 for latency tasks)
    dp_degree: int        # DP
    mp_degree: int        # MP
    mp_local: bool        # True = intra-node MP; False = cross-server MP

    @property
    def gpu_demand(self) -> int:
        """GPUs consumed by this config on the primary server."""
        return self.dp_degree * self.mp_degree

    # ---- throughput / latency helpers used by planner & server ----

    def effective_throughput(self, base_service_time_ms: float, is_frequency: bool) -> float:
        """Items per second under full-batch steady state."""
        bs = max(1, self.batch_size)
        items = bs * self.mf_width if is_frequency else bs
        time_s = self._batch_time(base_service_time_ms, items, is_frequency)
        return items / max(time_s, 1e-9)

    def estimated_time_s(self, base_service_time_ms: float, batch_items: int, is_frequency: bool) -> float:
        """Estimated wall-clock seconds for *batch_items* items."""
        return self._batch_time(base_service_time_ms, batch_items, is_frequency)

    def _batch_time(self, base_ms: float, n: int, is_freq: bool) -> float:
        n = max(1, n)
        batch_gain = max(1.0, min(self.batch_size, n) ** 0.55)
        mt_gain = 1.0 + 0.6 * max(0, self.mt_degree - 1)
        mf_gain = 1.0
        if is_freq and self.mf_width > 1:
            mf_gain = 1.0 + 0.25 * (min(self.mf_width, n) - 1)
        dp_gain = self.dp_degree * 0.92 if self.dp_degree > 1 else 1.0
        mp_gain = 1.0 + 0.75 * max(0, self.mp_degree - 1)
        total_gain = batch_gain * mt_gain * mf_gain * dp_gain * mp_gain
        return (base_ms / 1000.0) * n / total_gain

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service": self.service,
            "server": self.server,
            "batch_size": self.batch_size,
            "mt_degree": self.mt_degree,
            "mf_width": self.mf_width,
            "dp_degree": self.dp_degree,
            "mp_degree": self.mp_degree,
            "mp_local": self.mp_local,
            "gpu_demand": self.gpu_demand,
        }


# ---------------------------------------------------------------------------
# TaskResourceAllocator – resolves operator configs for placements
# ---------------------------------------------------------------------------

class TaskResourceAllocator:
    """Decide BS / MT / MF / DP / MP for each (service, server) placement.

    The allocator reads static profile defaults and adapts them based on
    server resource limits and optional real-time server states.
    """

    def __init__(
        self,
        servers: Mapping[str, "ServerResource"],
        services: Mapping[str, "ServiceProfile"],
        forecasts: Sequence["WorkloadForecast"] = (),
    ) -> None:
        self.servers = dict(servers)
        self.services = dict(services)
        self.forecasts = list(forecasts)

    def resolve(
        self,
        placement: "PlacementChoice",
        server_states: Optional[Mapping[str, Dict[str, Any]]] = None,
    ) -> OperatorConfig:
        from farfly_core import ServiceProfile  # deferred to avoid circular

        profile: ServiceProfile = self.services[placement.service]
        server = self.servers[placement.primary_server]

        # --- base from profile ---
        bs = profile.batch_size
        mt = profile.mt_degree
        mf = profile.multiframe_batch_width if profile.is_frequency else 1
        mp = profile.mp_degree
        mp_local = not profile.allow_cross_server_mp or len(placement.server_ids) == 1

        # --- constrain MP by available GPUs ---
        if mp_local:
            mp = min(mp, server.gpu_count)
        else:
            mp = min(mp, len(placement.server_ids))

        # --- compute DP from profile demand and remaining GPUs ---
        avail_for_dp = max(1, server.gpu_count // max(1, mp)) if mp_local else 1
        dp = min(profile.dp_groups, avail_for_dp)

        # --- BS adaptation under queue pressure ---
        if server_states:
            st = server_states.get(placement.primary_server, {})
            pressure = st.get("queued_batches", 0) / max(server.queue_capacity, 1)
            if pressure > 0.6:
                bs = min(bs * 2, 64)

        return OperatorConfig(
            service=placement.service,
            server=placement.primary_server,
            batch_size=bs,
            mt_degree=mt,
            mf_width=mf,
            dp_degree=dp,
            mp_degree=mp,
            mp_local=mp_local,
        )

    def resolve_all(
        self,
        placements: Sequence["PlacementChoice"],
        server_states: Optional[Mapping[str, Dict[str, Any]]] = None,
    ) -> Dict[Tuple[str, str], OperatorConfig]:
        result: Dict[Tuple[str, str], OperatorConfig] = {}
        for p in placements:
            result[(p.service, p.primary_server)] = self.resolve(p, server_states)
        return result
