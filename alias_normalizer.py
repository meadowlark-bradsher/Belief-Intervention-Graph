from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


ROLE_PATTERNS: list[tuple[str, str]] = [
    ("service", r"-service-\d+$"),
    ("pod", r"-pod-\d+$"),
    ("deployment", r"-deployment-\d+$"),
    ("statefulset", r"-statefulset-\d+$"),
    ("namespace", r"-namespace-\d+$"),
    ("node", r"-node-\d+$"),
]


def _strip_runtime_suffix(name: str) -> str:
    s = name.lower().strip()
    if "/" in s:
        s = s.split("/")[-1]
    s = re.sub(r"-[a-f0-9]{8,10}-[a-z0-9]{5}$", "", s)
    s = re.sub(r"-[a-f0-9]{9,10}$", "", s)
    s = re.sub(r"-\d+$", "", s)
    return s


def _group_component(group_id: str) -> str:
    s = group_id.lower()
    for _, pat in ROLE_PATTERNS:
        s = re.sub(pat, "", s)
    s = re.sub(r"-\d+$", "", s)
    return s


def _group_role(group_id: str) -> str:
    s = group_id.lower()
    for role, pat in ROLE_PATTERNS:
        if re.search(pat, s):
            return role
    return "other"


def _live_role(entity_name: str) -> str:
    parts = entity_name.split("/")
    if len(parts) >= 3:
        kind = parts[1].lower()
        if kind in {"service", "pod", "deployment", "statefulset", "namespace", "node", "schedule"}:
            return kind
    ls = entity_name.lower()
    if "service" in ls:
        return "service"
    if "deployment" in ls:
        return "deployment"
    if "pod" in ls:
        return "pod"
    if "node" in ls:
        return "node"
    return "other"


def _token_variants(name: str) -> set[str]:
    base = _strip_runtime_suffix(name)
    out = {base}
    skip = {"service", "pod", "deployment", "statefulset", "namespace", "node"}
    out.update(
        t
        for t in re.split(r"[^a-z0-9]+", base)
        if t and len(t) > 2 and not t.isdigit() and t not in skip
    )
    return out


@dataclass(slots=True)
class AliasNormalizer:
    component_tokens: dict[str, str]
    component_order: list[str]
    group_filter_rules: dict[str, list[re.Pattern[str]]]

    @classmethod
    def from_groundtruth(cls, gt: dict) -> "AliasNormalizer":
        groups = gt.get("spec", {}).get("groups", [])
        aliases = gt.get("spec", {}).get("aliases", [])

        component_tokens: dict[str, str] = {}
        group_filter_rules: dict[str, list[re.Pattern[str]]] = {}

        for group in groups:
            gid = group["id"]
            comp = _group_component(gid)
            for tok in _token_variants(gid):
                component_tokens[tok] = comp
            for rule in group.get("filter", []):
                if not rule:
                    continue
                try:
                    group_filter_rules.setdefault(comp, []).append(re.compile(rule, re.IGNORECASE))
                except re.error:
                    # Ground-truth filters can be permissive; ignore invalid regex patterns.
                    continue

        for alias_set in aliases:
            alias_components = [_group_component(a) for a in alias_set]
            canonical = sorted(alias_components, key=lambda x: (-len(x), x))[0]
            for alias in alias_set:
                for tok in _token_variants(alias):
                    component_tokens[tok] = canonical

        # Longest-first prevents "front" matching before "frontend-proxy".
        component_order = sorted(set(component_tokens.values()), key=lambda x: (-len(x), x))
        return cls(
            component_tokens=component_tokens,
            component_order=component_order,
            group_filter_rules=group_filter_rules,
        )

    def canonical_component(self, live_entity_name: str) -> str:
        raw = live_entity_name.lower().strip()
        base = _strip_runtime_suffix(raw)

        # 1) direct alias-token lookup from entity parts
        token_candidates: set[str] = set()
        for tok in _token_variants(base):
            if tok in self.component_tokens:
                token_candidates.add(self.component_tokens[tok])
        if token_candidates:
            return sorted(token_candidates, key=lambda x: (-len(x), x))[0]

        # 2) component-name substring lookup
        for comp in self.component_order:
            if comp in base:
                return comp

        # 3) regex group filters
        for comp, rules in self.group_filter_rules.items():
            for rule in rules:
                if rule.search(base) or rule.search(raw):
                    return comp

        return base

    def canonical_entity(self, live_entity_name: str) -> str:
        return f"{self.canonical_component(live_entity_name)}|{_live_role(live_entity_name)}"

    def canonical_gold_component(self, group_id: str) -> str:
        # Keep canonicalization consistent with live mapping.
        base = _group_component(group_id)
        return self.component_tokens.get(base, base)

    def canonical_gold_entity(self, group_id: str) -> str:
        return f"{self.canonical_gold_component(group_id)}|{_group_role(group_id)}"

    def components_from_text(self, text: str) -> set[str]:
        lower = text.lower()
        found: set[str] = set()
        for token, comp in self.component_tokens.items():
            if len(token) <= 2:
                continue
            if re.search(rf"\b{re.escape(token)}\b", lower):
                found.add(comp)
        if found:
            return found

        for comp in self.component_order:
            if comp in lower:
                found.add(comp)
        return found

    def canonicalize_many(self, entity_names: Iterable[str]) -> list[str]:
        return [self.canonical_entity(x) for x in entity_names]
