from __future__ import annotations

from alias_normalizer import AliasNormalizer


def _scenario_34_gt() -> dict:
    return {
        "spec": {
            "aliases": [
                ["valkey-service-1", "valkey-pod-1"],
                ["cart-service-1", "cart-pod-1"],
                ["checkout-service-1", "checkout-pod-1"],
                ["frontend-service-1", "frontend-pod-1"],
                ["frontend-proxy-service-1", "frontend-proxy-pod-1"],
            ],
            "groups": [
                {"id": "valkey-pod-1", "filter": ["valkey"]},
                {"id": "valkey-service-1", "filter": ["valkey"]},
                {"id": "cart-service-1", "filter": ["cart\\b"]},
                {"id": "cart-pod-1", "filter": ["cart-.*"]},
                {"id": "checkout-service-1", "filter": ["checkout\\b"]},
                {"id": "checkout-pod-1", "filter": ["checkout-.*"]},
                {"id": "frontend-service-1", "filter": ["frontend\\b"]},
                {"id": "frontend-pod-1", "filter": ["frontend-.*"]},
                {"id": "frontend-proxy-service-1", "filter": ["frontend-proxy\\b"]},
                {"id": "frontend-proxy-pod-1", "filter": ["frontend-proxy-.*"]},
            ],
        }
    }


def _scenario_16_gt() -> dict:
    return {
        "spec": {
            "aliases": [["shipping-service-1", "shipping-pod-1", "shipping-deployment-1"]],
            "groups": [
                {"id": "shipping-deployment-1", "filter": ["shipping\\b"]},
                {"id": "shipping-service-1", "filter": ["shipping\\b"]},
                {"id": "shipping-pod-1", "filter": ["shipping-.*"]},
            ],
        }
    }


def test_scenario_34_alias_canonicalization() -> None:
    normalizer = AliasNormalizer.from_groundtruth(_scenario_34_gt())

    assert normalizer.canonical_component("valkey-pod-1") == "valkey"
    assert normalizer.canonical_component("valkey-service-1") == "valkey"
    assert normalizer.canonical_component("otel-demo/Pod/valkey-cart-58df56c79c-bf4lr") == "valkey"
    assert normalizer.canonical_component("otel-demo/Service/valkey-cart") == "valkey"
    assert normalizer.canonical_component("otel-demo/Service/frontend-proxy") == "frontend-proxy"
    assert normalizer.canonical_component("otel-demo/Pod/frontend-proxy-6b4d584985-6rwg6") == "frontend-proxy"
    assert normalizer.canonical_component("otel-demo/Service/checkout") == "checkout"
    assert normalizer.canonical_component("otel-demo/Service/cart") == "cart"


def test_scenario_16_alias_canonicalization() -> None:
    normalizer = AliasNormalizer.from_groundtruth(_scenario_16_gt())

    assert normalizer.canonical_component("shipping-deployment-1") == "shipping"
    assert normalizer.canonical_component("shipping-pod-1") == "shipping"
    assert normalizer.canonical_component("shipping-service-1") == "shipping"
    assert normalizer.canonical_component("otel-demo/Deployment/shipping") == "shipping"
    assert normalizer.canonical_component("otel-demo/Pod/shipping-75d58f5d84-9rnq8") == "shipping"
