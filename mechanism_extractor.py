from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from alias_normalizer import AliasNormalizer


TAG_RULES: dict[str, re.Pattern[str]] = {
    "auth_failure": re.compile(
        r"\b(noauth|wrong password|invalid password|authentication failed|auth failed|permission denied)\b",
        re.IGNORECASE,
    ),
    "oom_killed": re.compile(r"\b(oomkilled|out of memory|oom kill)\b", re.IGNORECASE),
    "timeout": re.compile(
        r"\b(timeout|timed out|deadline exceeded|context deadline exceeded|i/o timeout)\b",
        re.IGNORECASE,
    ),
    "connection_refused": re.compile(r"\b(connection refused|connect: connection refused)\b", re.IGNORECASE),
    "dns_or_name_resolution_failure": re.compile(
        r"\b(nxdomain|no such host|host not found|name or service not known|temporary failure in name resolution)\b",
        re.IGNORECASE,
    ),
    "probe_failure": re.compile(r"\b(readiness probe failed|liveness probe failed|probe failed)\b", re.IGNORECASE),
    "image_pull_failure": re.compile(r"\b(imagepullbackoff|errimagepull|failed to pull image)\b", re.IGNORECASE),
    "crash_loop": re.compile(r"\b(crashloopbackoff|back-off restarting failed container)\b", re.IGNORECASE),
    "http_abort_or_reset": re.compile(
        r"\b(http[^\\n]{0,30}abort|stream reset|connection reset|reset by peer|upstream reset)\b",
        re.IGNORECASE,
    ),
    "resource_exhaustion": re.compile(
        r"\b(cpu throttling|cputhrottlinghigh|throttle_pct|resource exhausted|memory pressure|disk pressure)\b",
        re.IGNORECASE,
    ),
    "pending_unschedulable": re.compile(
        r"\b(unschedulable|failed scheduling|pending pods detected|0/\d+ nodes are available)\b",
        re.IGNORECASE,
    ),
}


SOURCE_CONFIDENCE = {
    "agent_entity": 0.75,
    "agent_propagation": 0.70,
    "agent_alert": 0.55,
    "session_output": 0.60,
}


@dataclass(slots=True)
class EvidenceText:
    source: str
    text: str
    component_hint: str | None = None
    timestamp: str | None = None


def _flatten_strings(value: Any) -> list[str]:
    out: list[str] = []
    if value is None:
        return out
    if isinstance(value, str):
        out.append(value)
        return out
    if isinstance(value, dict):
        for v in value.values():
            out.extend(_flatten_strings(v))
        return out
    if isinstance(value, list):
        for v in value:
            out.extend(_flatten_strings(v))
        return out
    return out


def _extract_session_output_strings(run_dir: Path) -> list[EvidenceText]:
    session_path = run_dir / "session.jsonl"
    if not session_path.exists():
        return []

    out: list[EvidenceText] = []
    with session_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            if row.get("type") != "response_item":
                continue
            payload = row.get("payload", {})
            if payload.get("type") != "function_call_output":
                continue

            text_candidates = _flatten_strings(payload.get("output"))
            ts = row.get("timestamp")

            # Many outputs are stringified JSON wrappers: {"output":"..."}.
            for txt in text_candidates:
                txt = txt.strip()
                if not txt:
                    continue
                expanded: list[str] = [txt]
                try:
                    parsed = json.loads(txt)
                    expanded = _flatten_strings(parsed)
                except Exception:
                    pass
                for e in expanded:
                    e = e.strip()
                    if not e:
                        continue
                    out.append(EvidenceText(source="session_output", text=e, timestamp=ts))
    return out


def _extract_agent_strings(
    agent_payload: dict[str, Any],
    normalizer: AliasNormalizer,
) -> list[EvidenceText]:
    out: list[EvidenceText] = []

    for entity in agent_payload.get("entities", []) or []:
        name = entity.get("name", "")
        comp_hint = normalizer.canonical_component(name) if name else None
        for field in ("reasoning", "evidence", "name"):
            txt = entity.get(field, "")
            if txt:
                out.append(EvidenceText(source="agent_entity", text=txt, component_hint=comp_hint))

    for prop in agent_payload.get("propagations", []) or []:
        src = prop.get("source", "")
        tgt = prop.get("target", "")
        src_comp = normalizer.canonical_component(src) if src else None
        tgt_comp = normalizer.canonical_component(tgt) if tgt else None
        for field in ("condition", "effect"):
            txt = prop.get(field, "")
            if txt:
                out.append(EvidenceText(source="agent_propagation", text=txt, component_hint=src_comp))
        if src:
            out.append(EvidenceText(source="agent_propagation", text=src, component_hint=src_comp))
        if tgt:
            out.append(EvidenceText(source="agent_propagation", text=tgt, component_hint=tgt_comp))

    for alert in agent_payload.get("alerts_explained", []) or []:
        txt = alert.get("explanation", "")
        if txt:
            out.append(EvidenceText(source="agent_alert", text=txt))
    return out


def _matched_tags(text: str) -> list[str]:
    tags: list[str] = []
    for tag, pattern in TAG_RULES.items():
        if pattern.search(text):
            tags.append(tag)
    return tags


def extract_mechanism_tags(
    run_dir: Path,
    normalizer: AliasNormalizer,
    agent_payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if agent_payload is None:
        agent_path = run_dir / "agent_output.json"
        if not agent_path.exists():
            return []
        agent_payload = json.loads(agent_path.read_text())

    evidences: list[EvidenceText] = []
    evidences.extend(_extract_agent_strings(agent_payload, normalizer))
    evidences.extend(_extract_session_output_strings(run_dir))

    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for idx, ev in enumerate(evidences):
        tags = _matched_tags(ev.text)
        if not tags:
            continue

        components = set()
        if ev.component_hint:
            components.add(ev.component_hint)
        components.update(normalizer.components_from_text(ev.text))
        if not components:
            continue

        for comp in sorted(components):
            for tag in tags:
                key = (comp, tag, ev.source)
                if key in seen:
                    continue
                seen.add(key)
                conf = SOURCE_CONFIDENCE.get(ev.source, 0.5)
                if len(tags) > 1:
                    conf += 0.10
                conf = min(1.0, conf)
                rows.append(
                    {
                        "component": comp,
                        "mechanism_tag": tag,
                        "evidence_source": ev.source,
                        "evidence": ev.text[:240],
                        "timestamp": ev.timestamp,
                        "confidence": round(conf, 3),
                        "observation_index": idx,
                    }
                )

    rows.sort(key=lambda r: (r["observation_index"], -r["confidence"], r["component"], r["mechanism_tag"]))
    return rows


def summarize_mechanism_tags(tags: Iterable[dict[str, Any]]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for t in tags:
        comp = t["component"]
        tag = t["mechanism_tag"]
        out.setdefault(comp, {})
        out[comp][tag] = max(out[comp].get(tag, 0.0), float(t.get("confidence", 0.0)))
    return out
