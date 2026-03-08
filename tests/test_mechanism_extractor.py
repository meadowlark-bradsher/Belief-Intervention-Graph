from __future__ import annotations

import json
from pathlib import Path

from alias_normalizer import AliasNormalizer
from mechanism_extractor import extract_mechanism_tags


def _normalizer() -> AliasNormalizer:
    gt = {
        "spec": {
            "aliases": [
                ["valkey-service-1", "valkey-pod-1"],
                ["shipping-service-1", "shipping-pod-1", "shipping-deployment-1"],
            ],
            "groups": [
                {"id": "valkey-pod-1", "filter": ["valkey"]},
                {"id": "valkey-service-1", "filter": ["valkey"]},
                {"id": "shipping-service-1", "filter": ["shipping"]},
                {"id": "shipping-pod-1", "filter": ["shipping"]},
                {"id": "shipping-deployment-1", "filter": ["shipping"]},
            ],
        }
    }
    return AliasNormalizer.from_groundtruth(gt)


def test_mechanism_tags_from_agent_and_session(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True)

    agent_payload = {
        "entities": [
            {
                "name": "otel-demo/Service/valkey-cart",
                "contributing_factor": True,
                "reasoning": "Valkey reports NOAUTH and wrong password during requests.",
                "evidence": "NOAUTH Authentication failed for cart access.",
            },
            {
                "name": "otel-demo/Service/shipping",
                "contributing_factor": False,
                "reasoning": "shipping requests hit deadline exceeded and connection refused",
                "evidence": "context deadline exceeded; connect: connection refused",
            },
        ],
        "propagations": [
            {
                "source": "otel-demo/Service/valkey-cart",
                "target": "otel-demo/Service/shipping",
                "condition": "Readiness probe failed due to connection refused",
                "effect": "shipping timeout observed",
            }
        ],
        "alerts_explained": [
            {
                "alert": "ImagePullBackOff",
                "explanation": "ErrImagePull seen in ad pod",
                "explained": True,
            }
        ],
    }
    (run_dir / "agent_output.json").write_text(json.dumps(agent_payload))

    session_rows = [
        {
            "timestamp": "2026-01-01T00:00:00Z",
            "type": "response_item",
            "payload": {
                "type": "function_call_output",
                "output": json.dumps(
                    {
                        "output": (
                            "otel-demo/Pod/valkey-cart-58df56c79c-bf4lr had throttle_pct 65.0; "
                            "otel-demo/Pod/shipping-75d58f5d84-9rnq8 was OOMKilled"
                        )
                    }
                ),
            },
        }
    ]
    with (run_dir / "session.jsonl").open("w", encoding="utf-8") as fh:
        for row in session_rows:
            fh.write(json.dumps(row) + "\n")

    tags = extract_mechanism_tags(run_dir, _normalizer())
    found = {(t["component"], t["mechanism_tag"]) for t in tags}

    assert ("valkey", "auth_failure") in found
    assert ("shipping", "timeout") in found
    assert ("shipping", "connection_refused") in found
    assert ("valkey", "resource_exhaustion") in found
    assert ("shipping", "oom_killed") in found
    assert any(t["timestamp"] == "2026-01-01T00:00:00Z" for t in tags)
