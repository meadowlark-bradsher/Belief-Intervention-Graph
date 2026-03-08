from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from alias_normalizer import AliasNormalizer
from mechanism_extractor import summarize_mechanism_tags


# consumer -> provider (consumer depends on provider)
DEFAULT_DEPENDENCIES: list[tuple[str, str]] = [
    ("frontend-proxy", "frontend"),
    ("frontend", "checkout"),
    ("checkout", "cart"),
    ("checkout", "shipping"),
    ("shipping", "quote"),
    ("cart", "valkey"),
    ("checkout", "payment"),
    ("checkout", "email"),
    ("checkout", "currency"),
    ("checkout", "fraud-detection"),
]


@dataclass(slots=True)
class EdgeCandidate:
    source_component: str
    target_component: str
    edge_type: str
    mechanism_tag: str | None = None
    evidence: list[str] | None = None
    confidence: float = 0.0


def _confidence(
    has_topology: bool,
    has_timing: bool,
    has_mechanism: bool,
    has_evidence: bool,
) -> float:
    points = int(has_topology) + int(has_timing) + int(has_mechanism) + int(has_evidence)
    return round(points / 4.0, 3)


def build_edge_candidates(
    agent_payload: dict[str, Any],
    normalizer: AliasNormalizer,
    mechanism_tags: list[dict[str, Any]],
) -> list[EdgeCandidate]:
    tag_summary = summarize_mechanism_tags(mechanism_tags)
    first_seen: dict[str, int] = {}
    for t in mechanism_tags:
        comp = t["component"]
        idx = int(t.get("observation_index", 10**9))
        first_seen[comp] = min(first_seen.get(comp, idx), idx)

    component_set = set()
    impacted_components: set[str] = set()
    for entity in agent_payload.get("entities", []) or []:
        comp = normalizer.canonical_component(entity.get("name", ""))
        if not comp:
            continue
        component_set.add(comp)
        if not entity.get("contributing_factor", False):
            impacted_components.add(comp)

    dep_provider_to_consumers: dict[str, set[str]] = {}
    for consumer, provider in DEFAULT_DEPENDENCIES:
        dep_provider_to_consumers.setdefault(provider, set()).add(consumer)

    edge_map: dict[tuple[str, str, str, str | None], EdgeCandidate] = {}

    def upsert(edge: EdgeCandidate) -> None:
        key = (edge.source_component, edge.target_component, edge.edge_type, edge.mechanism_tag)
        if key not in edge_map:
            edge_map[key] = edge
            return
        prior = edge_map[key]
        if edge.confidence > prior.confidence:
            prior.confidence = edge.confidence
        prior.evidence = sorted(set((prior.evidence or []) + (edge.evidence or [])))

    # 0) Static topology-template dependencies for stable structural skeleton.
    for consumer, provider in DEFAULT_DEPENDENCIES:
        if consumer in component_set and provider in component_set:
            upsert(
                EdgeCandidate(
                    source_component=consumer,
                    target_component=provider,
                    edge_type="depends_on",
                    mechanism_tag=None,
                    evidence=[f"topology_template:{consumer}->depends_on->{provider}"],
                    confidence=_confidence(
                        has_topology=True,
                        has_timing=False,
                        has_mechanism=False,
                        has_evidence=True,
                    ),
                )
            )

    # 1) Direct propagation statements from live agent output.
    for prop in agent_payload.get("propagations", []) or []:
        src = normalizer.canonical_component(prop.get("source", ""))
        tgt = normalizer.canonical_component(prop.get("target", ""))
        if not src or not tgt:
            continue
        component_set.update({src, tgt})

        cond = prop.get("condition", "")
        eff = prop.get("effect", "")
        has_evidence = bool(cond or eff)
        has_mech = src in tag_summary
        conf = _confidence(
            has_topology=True,
            has_timing=False,
            has_mechanism=has_mech,
            has_evidence=has_evidence,
        )
        upsert(
            EdgeCandidate(
                source_component=src,
                target_component=tgt,
                edge_type="propagates_to",
                mechanism_tag=None,
                evidence=[x for x in [cond, eff] if x],
                confidence=conf,
            )
        )

        # Reverse structural dependency.
        upsert(
            EdgeCandidate(
                source_component=tgt,
                target_component=src,
                edge_type="depends_on",
                mechanism_tag=None,
                evidence=[x for x in [cond, eff] if x],
                confidence=_confidence(
                    has_topology=True,
                    has_timing=False,
                    has_mechanism=False,
                    has_evidence=has_evidence,
                ),
            )
        )

    # 2) Component exhibits mechanism.
    for comp, tags in tag_summary.items():
        component_set.add(comp)
        for tag, conf in tags.items():
            upsert(
                EdgeCandidate(
                    source_component=comp,
                    target_component=f"mechanism:{tag}",
                    edge_type="exhibits_mechanism",
                    mechanism_tag=tag,
                    evidence=[f"tag:{tag}"],
                    confidence=round(float(conf), 3),
                )
            )

    # 3) Topology + mechanism inferred propagation.
    # If provider has mechanism tags and consumer appears impacted, infer provider -> consumer.
    for provider, consumers in dep_provider_to_consumers.items():
        if provider not in tag_summary:
            continue
        for consumer in consumers:
            if consumer not in impacted_components and consumer not in component_set:
                continue
            timing_supported = (
                provider in first_seen
                and consumer in first_seen
                and first_seen[provider] <= first_seen[consumer]
            )
            strongest_tag = max(tag_summary[provider].items(), key=lambda x: x[1])[0]
            upsert(
                EdgeCandidate(
                    source_component=provider,
                    target_component=consumer,
                    edge_type="propagates_to",
                    mechanism_tag=strongest_tag,
                    evidence=[f"dependency:{consumer}->depends_on->{provider}", f"mechanism:{strongest_tag}"],
                    confidence=_confidence(
                        has_topology=True,
                        has_timing=timing_supported,
                        has_mechanism=True,
                        has_evidence=True,
                    ),
                )
            )
            upsert(
                EdgeCandidate(
                    source_component=consumer,
                    target_component=provider,
                    edge_type="depends_on",
                    mechanism_tag=None,
                    evidence=[f"dependency:{consumer}->depends_on->{provider}"],
                    confidence=_confidence(
                        has_topology=True,
                        has_timing=False,
                        has_mechanism=False,
                        has_evidence=True,
                    ),
                )
            )

    return sorted(
        edge_map.values(),
        key=lambda e: (e.edge_type, e.source_component, e.target_component, -(e.confidence or 0.0)),
    )


def edge_candidates_to_rows(edges: list[EdgeCandidate]) -> list[dict[str, Any]]:
    return [asdict(e) for e in edges]
