from __future__ import annotations

import json
from typing import Any, Dict, Tuple

import farfly_pb2


def grpc_target(ip: str, port: int) -> str:
    return f"{ip}:{int(port)}"


def dump_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def make_json_request(payload: Dict[str, Any]) -> farfly_pb2.JsonRequest:
    return farfly_pb2.JsonRequest(json_payload=dump_json(payload))


def parse_json_request(request: farfly_pb2.JsonRequest) -> Dict[str, Any]:
    if not request.json_payload:
        return {}
    return json.loads(request.json_payload)


def make_json_reply(payload: Dict[str, Any], status_code: int = 200) -> farfly_pb2.JsonReply:
    return farfly_pb2.JsonReply(status_code=int(status_code), json_payload=dump_json(payload))


def parse_json_reply(reply: farfly_pb2.JsonReply) -> Tuple[int, Dict[str, Any]]:
    payload = json.loads(reply.json_payload) if reply.json_payload else {}
    return int(reply.status_code or 200), payload