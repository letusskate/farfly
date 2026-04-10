from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import grpc

import farfly_pb2
import farfly_pb2_grpc
from farfly_rpc import grpc_target, make_json_request, parse_json_reply


def load_json_file(file_path: str) -> Dict[str, Any]:
    path = Path(file_path).resolve()
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


class SchedulerClient:
    def __init__(self, ip: str, port: int) -> None:
        self.ip = str(ip)
        self.port = int(port)
        self.channel = grpc.insecure_channel(grpc_target(self.ip, self.port))
        self.stub = farfly_pb2_grpc.FarflySchedulerServiceStub(self.channel)

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "SchedulerClient":
        scheduler = config["scheduler"]
        return cls(str(scheduler["ip"]), int(scheduler["port"]))

    @classmethod
    def from_config_file(cls, config_file: str) -> "SchedulerClient":
        return cls.from_config(load_json_file(config_file))

    def close(self) -> None:
        self.channel.close()

    def _call(self, method_name: str, payload: Optional[Dict[str, Any]], timeout: Optional[float]) -> Tuple[int, Dict[str, Any]]:
        method = getattr(self.stub, method_name)
        request = farfly_pb2.Empty() if payload is None else make_json_request(payload)
        reply = method(request, timeout=timeout)
        return parse_json_reply(reply)

    def health(self, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("Health", None, timeout)

    def metrics(self, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("Metrics", None, timeout)

    def plan(self, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("Plan", None, timeout)

    def recompute_plan(self, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("RecomputePlan", None, timeout)

    def reload_plan(self, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("ReloadPlan", None, timeout)

    def submit(self, payload: Dict[str, Any], timeout: float = 30.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("Submit", payload, timeout)

    def report_credits(self, payload: Dict[str, Any], timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("ReportCredits", payload, timeout)


class ServerClient:
    def __init__(self, ip: str, port: int) -> None:
        self.ip = str(ip)
        self.port = int(port)
        self.channel = grpc.insecure_channel(grpc_target(self.ip, self.port))
        self.stub = farfly_pb2_grpc.ServerServiceStub(self.channel)

    @classmethod
    def from_server_definition(cls, item: Dict[str, Any]) -> "ServerClient":
        return cls(str(item["ip"]), int(item["port"]))

    @classmethod
    def from_server_config_file(cls, config_file: str) -> "ServerClient":
        config = load_json_file(config_file)
        self_config = config["self"]
        ip = str(self_config.get("ip", "127.0.0.1"))
        if ip in {"0.0.0.0", "::"}:
            ip = str(self_config.get("exposed_ip", ip))
        return cls(ip, int(self_config["port"]))

    def close(self) -> None:
        self.channel.close()

    def _call(self, method_name: str, payload: Optional[Dict[str, Any]], timeout: Optional[float]) -> Tuple[int, Dict[str, Any]]:
        method = getattr(self.stub, method_name)
        request = farfly_pb2.Empty() if payload is None else make_json_request(payload)
        reply = method(request, timeout=timeout)
        return parse_json_reply(reply)

    def health(self, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("Health", None, timeout)

    def metrics(self, timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("Metrics", None, timeout)

    def update_placement(self, payload: Dict[str, Any], timeout: float = 5.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("UpdatePlacement", payload, timeout)

    def execute_batch(self, payload: Dict[str, Any], timeout: float = 30.0) -> Tuple[int, Dict[str, Any]]:
        return self._call("ExecuteBatch", payload, timeout)