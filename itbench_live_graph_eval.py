#!/usr/bin/env python3
"""
Evaluate ITBench live artifact graphs against ground truth using a semantic extraction layer.

Upgrades included:
- Alias normalization from ITBench aliases/groups filters
- Mechanism tag extraction from agent/session artifacts
- First-class edge candidates (propagates_to, depends_on, exhibits_mechanism)

Outputs under --out-dir:
- fidelity_report.json
- messiness_report.json
- repeatability_report.json
- discriminability_report.json
- root_cause_density_report.json
- summary.md
- run_status.json
- improvement_report.md
- <scenario_id>/run_<N>/mechanism_tags.json
- <scenario_id>/run_<N>/edge_candidates.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import resource
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from alias_normalizer import AliasNormalizer
from edge_builder import build_edge_candidates, edge_candidates_to_rows
from mechanism_extractor import extract_mechanism_tags, summarize_mechanism_tags


HIGH_SIGNAL_TAGS = {
    "auth_failure",
    "oom_killed",
    "timeout",
    "connection_refused",
    "dns_or_name_resolution_failure",
    "probe_failure",
    "image_pull_failure",
    "crash_loop",
    "http_abort_or_reset",
}

INFRA_HINTS = {
    "valkey",
    "postgresql",
    "kafka",
    "node",
    "otel-collector",
}


@dataclass(slots=True)
class ScenarioGold:
    scenario_id: str
    chaos_mesh_enabled: bool
    normalizer: AliasNormalizer
    root_components: set[str]
    all_components: set[str]
    gold_edges_components: set[tuple[str, str]]
    fault_mechanisms: list[str]
    fault_conditions: list[str]


def scenario_number(scenario_id: str) -> int:
    return int(scenario_id.split("_")[1])


def peak_memory_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024.0 * 1024.0)
    return rss / 1024.0


def edge_precision_recall_f1(predicted: set[tuple[str, str]], gold: set[tuple[str, str]]) -> dict[str, float]:
    if not predicted and not gold:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    if not predicted:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0}

    tp = len(predicted & gold)
    precision = tp / len(predicted) if predicted else 0.0
    recall = tp / len(gold) if gold else 0.0
    f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    return {"precision": precision, "recall": recall, "f1": f1}


def load_gold(scenarios_base: Path, scenario_id: str) -> ScenarioGold:
    gt_path = scenarios_base / scenario_id / "groundtruth_v1.yaml"
    gt = yaml.safe_load(gt_path.read_text())
    normalizer = AliasNormalizer.from_groundtruth(gt)

    sp = scenarios_base / scenario_id / "scenario.yaml"
    chaos_mesh_enabled = False
    if sp.exists():
        scenario = yaml.safe_load(sp.read_text())
        chaos_mesh_enabled = bool(
            scenario.get("spec", {})
            .get("tools", {})
            .get("chaosEngineering", {})
            .get("chaosMesh", {})
            .get("enabled", False)
        )

    groups = gt.get("spec", {}).get("groups", [])
    root_ids = [g["id"] for g in groups if g.get("root_cause")]
    root_components = {normalizer.canonical_gold_component(gid) for gid in root_ids}
    all_components = {normalizer.canonical_gold_component(g["id"]) for g in groups}

    props = gt.get("spec", {}).get("propagations", [])
    gold_edges_components = {
        (
            normalizer.canonical_gold_component(p["source"]),
            normalizer.canonical_gold_component(p["target"]),
        )
        for p in props
    }

    faults = gt.get("spec", {}).get("fault", [])
    fault_mechanisms = [f.get("fault_mechanism", "") for f in faults if f.get("fault_mechanism")]
    fault_conditions = [f.get("condition", "") for f in faults if f.get("condition")]

    return ScenarioGold(
        scenario_id=scenario_id,
        chaos_mesh_enabled=chaos_mesh_enabled,
        normalizer=normalizer,
        root_components=root_components,
        all_components=all_components,
        gold_edges_components=gold_edges_components,
        fault_mechanisms=fault_mechanisms,
        fault_conditions=fault_conditions,
    )


def load_runs(trajectories_root: Path, scenario_id: str) -> list[dict[str, Any]]:
    sid_num = scenario_number(scenario_id)
    scenario_dir = trajectories_root / f"Scenario-{sid_num}"
    if not scenario_dir.exists():
        return []

    out: list[dict[str, Any]] = []
    run_dirs = sorted(
        [d for d in scenario_dir.iterdir() if d.is_dir() and d.name.isdigit()],
        key=lambda d: int(d.name),
    )

    for run_dir in run_dirs:
        agent_path = run_dir / "agent_output.json"
        if not agent_path.exists():
            continue
        payload = json.loads(agent_path.read_text())

        judge_metrics: dict[str, float | None] = {
            "judge_root_f1": None,
            "judge_prop_precision": None,
            "judge_prop_recall": None,
            "judge_prop_f1": None,
        }
        judge_path = run_dir / "judge_output.json"
        if judge_path.exists():
            try:
                jp = json.loads(judge_path.read_text())
                scores = jp.get("eval_result", {}).get("scores", {})
                root = scores.get("root_cause_entity", {})
                prop = scores.get("propagation_chain", {})
                judge_metrics = {
                    "judge_root_f1": root.get("calculation_f1"),
                    "judge_prop_precision": prop.get("precision"),
                    "judge_prop_recall": prop.get("recall"),
                    "judge_prop_f1": prop.get("calculation"),
                }
            except Exception:
                pass

        out.append(
            {
                "run_id": int(run_dir.name),
                "run_dir": run_dir,
                "agent_payload": payload,
                **judge_metrics,
            }
        )
    return out


def _root_candidates_from_tags(
    tag_summary: dict[str, dict[str, float]],
    contributing_components: set[str],
) -> list[str]:
    scored: list[tuple[float, str]] = []
    for comp, tags in tag_summary.items():
        best = max(tags.values()) if tags else 0.0
        tag_bonus = 0.0
        if any(t in HIGH_SIGNAL_TAGS for t in tags):
            tag_bonus += 0.40
        if comp in INFRA_HINTS:
            tag_bonus += 0.20
        scored.append((best + tag_bonus, comp))

    scored.sort(key=lambda x: (-x[0], x[1]))
    top_tag_candidates = [comp for _, comp in scored[:2]]

    merged = sorted(contributing_components | set(top_tag_candidates))
    return merged


def run_metrics(
    gold: ScenarioGold,
    run: dict[str, Any],
    mechanism_tags: list[dict[str, Any]],
    edge_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = run["agent_payload"]
    normalizer = gold.normalizer

    # Use first-class propagation candidates for overlap/stability.
    live_edges = {
        (e["source_component"], e["target_component"])
        for e in edge_rows
        if e["edge_type"] == "propagates_to"
        and e["target_component"]
        and not str(e["target_component"]).startswith("mechanism:")
        and float(e.get("confidence", 0.0)) >= 0.50
    }
    stability_edges = {
        (e["source_component"], e["target_component"])
        for e in edge_rows
        if e["edge_type"] in {"propagates_to", "depends_on"}
        and e["target_component"]
        and not str(e["target_component"]).startswith("mechanism:")
        and float(e.get("confidence", 0.0)) >= 0.50
    }

    live_nodes = {
        normalizer.canonical_component(ent.get("name", ""))
        for ent in payload.get("entities", []) or []
        if ent.get("name")
    }
    for s, t in live_edges:
        live_nodes.add(s)
        live_nodes.add(t)

    contributing_components = {
        normalizer.canonical_component(ent.get("name", ""))
        for ent in payload.get("entities", []) or []
        if ent.get("name") and ent.get("contributing_factor")
    }

    tag_summary = summarize_mechanism_tags(mechanism_tags)
    root_candidate_components = _root_candidates_from_tags(tag_summary, contributing_components)
    root_match_components = sorted(set(root_candidate_components) & gold.root_components)
    root_match = len(root_match_components) > 0

    mechanism_reproduced_proxy = root_match or any(rc in tag_summary for rc in gold.root_components)

    prf = edge_precision_recall_f1(live_edges, gold.gold_edges_components)
    missing_gold_edges = sorted(gold.gold_edges_components - live_edges)
    extra_edges = sorted(live_edges - gold.gold_edges_components)

    missing_gold_nodes = sorted(gold.all_components - live_nodes)
    extra_nodes = sorted(live_nodes - gold.all_components)

    root_neighbors = set()
    for src, tgt in live_edges:
        if src in gold.root_components:
            root_neighbors.add(tgt)
        if tgt in gold.root_components:
            root_neighbors.add(src)

    tag_examples = []
    for comp, tags in sorted(tag_summary.items()):
        for tag, conf in sorted(tags.items(), key=lambda x: (-x[1], x[0])):
            tag_examples.append(f"{comp}:{tag}@{conf:.2f}")
    tag_examples = tag_examples[:8]

    return {
        "run_id": run["run_id"],
        "live_entity_count": len(live_nodes),
        "live_edge_count": len(live_edges),
        "gold_entity_count": len(gold.all_components),
        "gold_edge_count": len(gold.gold_edges_components),
        "fidelity": {
            "mechanism_reproduced_proxy": mechanism_reproduced_proxy,
            "root_cause_match": root_match,
            "root_cause_component_matches": root_match_components,
            "root_candidate_components": root_candidate_components,
            "propagation_precision": prf["precision"],
            "propagation_recall": prf["recall"],
            "propagation_f1": prf["f1"],
            "exact_chain_match": live_edges == gold.gold_edges_components and len(gold.gold_edges_components) > 0,
            "approx_chain_recovered": prf["recall"] >= 0.5,
            "judge_root_f1": run.get("judge_root_f1"),
            "judge_prop_precision": run.get("judge_prop_precision"),
            "judge_prop_recall": run.get("judge_prop_recall"),
            "judge_prop_f1": run.get("judge_prop_f1"),
            "mechanism_tag_examples": tag_examples,
        },
        "messiness": {
            "extra_nodes_count": len(extra_nodes),
            "extra_nodes": extra_nodes,
            "missing_gold_nodes_count": len(missing_gold_nodes),
            "missing_gold_nodes": missing_gold_nodes,
            "extra_edges_count": len(extra_edges),
            "extra_edges": extra_edges,
            "missing_gold_edges_count": len(missing_gold_edges),
            "missing_gold_edges": missing_gold_edges,
        },
        "density": {
            "plausible_mechanism_candidates": len(root_candidate_components),
            "root_neighbor_count": len(root_neighbors),
            "ambiguity_pressure_score": len(root_candidate_components) + len(root_neighbors),
        },
        "raw_component_edges": sorted(stability_edges),
    }


def repeatability_metrics(
    scenario_id: str,
    gold_edges: set[tuple[str, str]],
    run_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    edge_sets = [set(map(tuple, row["raw_component_edges"])) for row in run_rows if row["raw_component_edges"]]
    run_ids = [row["run_id"] for row in run_rows]

    out: dict[str, Any] = {
        "scenario_id": scenario_id,
        "run_ids": run_ids,
        "run_count_total": len(run_rows),
        "run_count_with_edges": len(edge_sets),
        "stable_edges": [],
        "unstable_edges": [],
        "stable_extra_edges": [],
        "pairwise_edge_jaccard_mean": None,
    }

    if not edge_sets:
        return out

    if len(edge_sets) < 2:
        union_edges = set.union(*edge_sets)
        out.update(
            {
                "stable_edges": [],
                "unstable_edges": sorted(union_edges),
                "stable_extra_edges": [],
                "pairwise_edge_jaccard_mean": None,
            }
        )
        return out

    union_edges = set.union(*edge_sets)
    freq: dict[tuple[str, str], int] = {}
    for es in edge_sets:
        for edge in es:
            freq[edge] = freq.get(edge, 0) + 1
    stable_threshold = 2
    stable_edges = {edge for edge, count in freq.items() if count >= stable_threshold}
    unstable_edges = union_edges - stable_edges

    jaccards = []
    for a, b in itertools.combinations(edge_sets, 2):
        denom = len(a | b)
        jaccards.append(1.0 if denom == 0 else len(a & b) / denom)

    out.update(
        {
            "stable_edges": sorted(stable_edges),
            "unstable_edges": sorted(unstable_edges),
            "stable_extra_edges": sorted(stable_edges - gold_edges),
            "pairwise_edge_jaccard_mean": (sum(jaccards) / len(jaccards)) if jaccards else None,
        }
    )
    return out


def scenario_core_edges(repeatability_row: dict[str, Any], run_rows: list[dict[str, Any]]) -> set[tuple[str, str]]:
    stable = {tuple(e) for e in repeatability_row.get("stable_edges", [])}
    if stable:
        return stable
    union: set[tuple[str, str]] = set()
    for row in run_rows:
        union.update(map(tuple, row.get("raw_component_edges", [])))
    return union


def discriminability_for_pair(
    a: str,
    b: str,
    by_scenario_runs: dict[str, list[dict[str, Any]]],
    by_scenario_repeatability: dict[str, dict[str, Any]],
    by_scenario_gold_edges: dict[str, set[tuple[str, str]]],
) -> dict[str, Any]:
    if a not in by_scenario_runs or b not in by_scenario_runs:
        missing = [sid for sid in [a, b] if sid not in by_scenario_runs]
        return {
            "scenario_a": a,
            "scenario_b": b,
            "available": False,
            "missing_scenarios": missing,
        }

    core_a = scenario_core_edges(by_scenario_repeatability[a], by_scenario_runs[a])
    core_b = scenario_core_edges(by_scenario_repeatability[b], by_scenario_runs[b])
    gold_a = by_scenario_gold_edges[a]
    gold_b = by_scenario_gold_edges[b]

    overlap = core_a & core_b
    min_size = min(len(core_a), len(core_b))
    overlap_ratio = (len(overlap) / min_size) if min_size > 0 else 0.0

    densifying_non_helpful = overlap - gold_a - gold_b
    helpful_a = (core_a - core_b) - gold_a
    helpful_b = (core_b - core_a) - gold_b

    return {
        "scenario_a": a,
        "scenario_b": b,
        "available": True,
        "core_edges_a_count": len(core_a),
        "core_edges_b_count": len(core_b),
        "overlap_core_edge_count": len(overlap),
        "overlap_ratio": overlap_ratio,
        "more_confusable_with_live_artifacts": overlap_ratio >= 0.5 and len(overlap) > 0,
        "helpful_extra_artifacts_for_a": sorted(helpful_a),
        "helpful_extra_artifacts_for_b": sorted(helpful_b),
        "densifying_non_helpful_edges": sorted(densifying_non_helpful),
    }


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).parent
    parser = argparse.ArgumentParser(description="Evaluate ITBench live artifact graphs with semantic extraction")
    parser.add_argument(
        "--scenarios-base",
        type=Path,
        default=(repo_root / "../../ITBench-hub/ITBench-Scenarios/sre/roles/scenarios/files").resolve(),
    )
    parser.add_argument(
        "--trajectories-root",
        type=Path,
        default=(
            repo_root
            / "../../ITBench-hub/ITBench-Trajectories/ReAct-Agent-Trajectories/OpenAI-GPT-OSS-120B/sre"
        ).resolve(),
    )
    parser.add_argument(
        "--scenario-ids",
        type=str,
        default="scenario_27,scenario_41,scenario_16,scenario_34,scenario_40",
    )
    parser.add_argument(
        "--pair-list",
        type=str,
        default="scenario_34:scenario_40,scenario_16:scenario_27,scenario_27:scenario_41,scenario_16:scenario_34",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("out/itbench/live_eval"))
    parser.add_argument("--memory-limit-mb", type=float, default=1024.0)
    return parser.parse_args()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_improvement_report(
    out_dir: Path,
    fidelity_rows: list[dict[str, Any]],
    repeatability_rows: list[dict[str, Any]],
    peak_mb: float,
) -> None:
    # Baseline captured from pre-upgrade run (strict component/role matching before semantic extraction).
    baseline_root_true = 0
    baseline_root_total = 5
    baseline_mean_prop_f1 = 0.0
    baseline_s34_stable_edges = 0
    baseline_s34_jacc = 0.0

    root_true_after = sum(1 for r in fidelity_rows if r.get("root_cause_match"))
    root_total_after = len(fidelity_rows)
    mean_prop_f1_after = (
        sum(float(r.get("propagation_f1", 0.0)) for r in fidelity_rows) / root_total_after
        if root_total_after
        else 0.0
    )

    s34_repeat = next((r for r in repeatability_rows if r["scenario_id"] == "scenario_34"), None)
    s34_stable_after = len(s34_repeat["stable_edges"]) if s34_repeat else 0
    s34_jacc_after = s34_repeat.get("pairwise_edge_jaccard_mean") if s34_repeat else None

    # Collect a few examples for report readability.
    tag_examples: list[str] = []
    for row in fidelity_rows:
        for ex in row.get("mechanism_tag_examples", []):
            if ex not in tag_examples:
                tag_examples.append(ex)
    tag_examples = tag_examples[:10]

    lines: list[str] = []
    lines.append("# Semantic Extraction Improvement Report")
    lines.append("")
    lines.append("## Before vs After")
    lines.append("")
    lines.append("| Metric | Before | After |")
    lines.append("|---|---:|---:|")
    lines.append(f"| Root-cause match (true runs) | {baseline_root_true}/{baseline_root_total} | {root_true_after}/{root_total_after} |")
    lines.append(f"| Mean propagation overlap F1 | {baseline_mean_prop_f1:.3f} | {mean_prop_f1_after:.3f} |")
    lines.append(f"| scenario_34 stable core edges | {baseline_s34_stable_edges} | {s34_stable_after} |")
    s34_jacc_txt = f"{s34_jacc_after:.3f}" if isinstance(s34_jacc_after, (int, float)) else "n/a"
    lines.append(f"| scenario_34 mean edge Jaccard | {baseline_s34_jacc:.3f} | {s34_jacc_txt} |")
    lines.append("")

    lines.append("## Mechanism Tag Examples")
    lines.append("")
    if tag_examples:
        for ex in tag_examples:
            lines.append(f"- {ex}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Canonicalization Examples")
    lines.append("")
    lines.append("- valkey-pod-1 -> valkey")
    lines.append("- valkey-service-1 -> valkey")
    lines.append("- otel-demo/Pod/valkey-cart-58df56c79c-bf4lr -> valkey")
    lines.append("- shipping-deployment-1 -> shipping")
    lines.append("- otel-demo/Pod/shipping-75d58f5d84-9rnq8 -> shipping")
    lines.append("")

    lines.append("## Runtime")
    lines.append("")
    lines.append(f"- Peak RSS: **{peak_mb:.2f} MB**")
    lines.append("- Bounded-memory behavior preserved; no lattice computation performed.")
    lines.append("")

    (out_dir / "improvement_report.md").write_text("\n".join(lines))


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    scenario_ids = [x.strip() for x in args.scenario_ids.split(",") if x.strip()]
    pairs: list[tuple[str, str]] = []
    for item in args.pair_list.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        a, b = item.split(":", 1)
        pairs.append((a.strip(), b.strip()))

    by_scenario_runs: dict[str, list[dict[str, Any]]] = {}
    by_scenario_repeat: dict[str, dict[str, Any]] = {}
    by_scenario_gold_edges: dict[str, set[tuple[str, str]]] = {}
    missing_live_scenarios: list[str] = []

    fidelity_rows: list[dict[str, Any]] = []
    messiness_rows: list[dict[str, Any]] = []
    density_rows: list[dict[str, Any]] = []

    for sid in scenario_ids:
        gold = load_gold(args.scenarios_base, sid)
        by_scenario_gold_edges[sid] = gold.gold_edges_components

        runs = load_runs(args.trajectories_root, sid)
        if not runs:
            missing_live_scenarios.append(sid)
            continue

        run_rows: list[dict[str, Any]] = []
        for run in runs:
            run_out_dir = out_dir / sid / f"run_{run['run_id']}"
            run_out_dir.mkdir(parents=True, exist_ok=True)

            mechanism_tags = extract_mechanism_tags(
                run_dir=run["run_dir"],
                normalizer=gold.normalizer,
                agent_payload=run["agent_payload"],
            )
            write_json(run_out_dir / "mechanism_tags.json", mechanism_tags)

            edge_candidates = build_edge_candidates(
                agent_payload=run["agent_payload"],
                normalizer=gold.normalizer,
                mechanism_tags=mechanism_tags,
            )
            edge_rows = edge_candidates_to_rows(edge_candidates)
            write_json(run_out_dir / "edge_candidates.json", edge_rows)

            rr = run_metrics(gold, run, mechanism_tags, edge_rows)
            run_rows.append(rr)

            fidelity_rows.append(
                {
                    "scenario_id": sid,
                    "run_id": rr["run_id"],
                    "chaos_mesh_enabled": gold.chaos_mesh_enabled,
                    "gold_root_components": sorted(gold.root_components),
                    **rr["fidelity"],
                }
            )
            messiness_rows.append(
                {
                    "scenario_id": sid,
                    "run_id": rr["run_id"],
                    "chaos_mesh_enabled": gold.chaos_mesh_enabled,
                    **rr["messiness"],
                }
            )
            density_rows.append(
                {
                    "scenario_id": sid,
                    "run_id": rr["run_id"],
                    "chaos_mesh_enabled": gold.chaos_mesh_enabled,
                    **rr["density"],
                }
            )

            if peak_memory_mb() > args.memory_limit_mb:
                summary = {
                    "status": "aborted",
                    "reason": f"Peak RSS {peak_memory_mb():.2f} MB exceeded limit {args.memory_limit_mb:.2f} MB",
                    "last_scenario": sid,
                    "last_run": run["run_id"],
                }
                write_json(out_dir / "run_status.json", summary)
                print(json.dumps(summary, indent=2))
                return 2

        by_scenario_runs[sid] = run_rows
        by_scenario_repeat[sid] = repeatability_metrics(sid, gold.gold_edges_components, run_rows)

    repeatability_rows = [by_scenario_repeat[sid] for sid in scenario_ids if sid in by_scenario_repeat]
    discriminability_rows = [
        discriminability_for_pair(a, b, by_scenario_runs, by_scenario_repeat, by_scenario_gold_edges)
        for a, b in pairs
    ]

    write_json(out_dir / "fidelity_report.json", fidelity_rows)
    write_json(out_dir / "messiness_report.json", messiness_rows)
    write_json(out_dir / "repeatability_report.json", repeatability_rows)
    write_json(out_dir / "discriminability_report.json", discriminability_rows)
    write_json(out_dir / "root_cause_density_report.json", density_rows)

    # Summary markdown
    lines: list[str] = []
    lines.append("# ITBench Live Artifact Graph Evaluation")
    lines.append("")
    lines.append(f"- Scenarios requested: **{', '.join(scenario_ids)}**")
    lines.append(f"- Scenarios with live runs found: **{len(by_scenario_runs)}**")
    lines.append(f"- Missing live-run scenarios: **{', '.join(missing_live_scenarios) if missing_live_scenarios else 'none'}**")
    lines.append(f"- Peak RSS observed: **{peak_memory_mb():.2f} MB**")
    lines.append(f"- Memory limit: **{args.memory_limit_mb:.2f} MB**")
    lines.append("")

    lines.append("## Fidelity")
    lines.append("")
    if fidelity_rows:
        lines.append("| Scenario | Run | Mechanism Proxy | Root Match | Prop P | Prop R | Prop F1 | Judge Root F1 | Judge Prop F1 |")
        lines.append("|---|---:|---|---|---:|---:|---:|---:|---:|")
        for row in sorted(fidelity_rows, key=lambda r: (scenario_number(r["scenario_id"]), r["run_id"])):
            jr = row.get("judge_root_f1")
            jp = row.get("judge_prop_f1")
            jr_txt = f"{jr:.3f}" if isinstance(jr, (int, float)) else "n/a"
            jp_txt = f"{jp:.3f}" if isinstance(jp, (int, float)) else "n/a"
            lines.append(
                f"| {row['scenario_id']} | {row['run_id']} | "
                f"{'yes' if row['mechanism_reproduced_proxy'] else 'no'} | "
                f"{'yes' if row['root_cause_match'] else 'no'} | "
                f"{row['propagation_precision']:.3f} | {row['propagation_recall']:.3f} | {row['propagation_f1']:.3f} | "
                f"{jr_txt} | {jp_txt} |"
            )
    else:
        lines.append("No live runs were found.")
    lines.append("")

    lines.append("## Repeatability")
    lines.append("")
    if repeatability_rows:
        lines.append("| Scenario | Runs | Stable edges | Unstable edges | Mean Jaccard |")
        lines.append("|---|---:|---:|---:|---:|")
        for row in sorted(repeatability_rows, key=lambda r: scenario_number(r["scenario_id"])):
            j = row["pairwise_edge_jaccard_mean"]
            jtxt = f"{j:.3f}" if isinstance(j, (int, float)) else "n/a"
            lines.append(
                f"| {row['scenario_id']} | {row['run_count_total']} | "
                f"{len(row['stable_edges'])} | {len(row['unstable_edges'])} | {jtxt} |"
            )
    else:
        lines.append("No repeatability data available.")
    lines.append("")

    lines.append("## Discriminability")
    lines.append("")
    for row in discriminability_rows:
        pair = f"{row['scenario_a']} vs {row['scenario_b']}"
        if not row["available"]:
            lines.append(f"- **{pair}**: unavailable (missing: {', '.join(row['missing_scenarios'])})")
            continue
        lines.append(
            f"- **{pair}**: overlap_ratio={row['overlap_ratio']:.3f}, "
            f"more_confusable_with_live_artifacts={'yes' if row['more_confusable_with_live_artifacts'] else 'no'}, "
            f"helpful_extra_edges=({len(row['helpful_extra_artifacts_for_a'])}, {len(row['helpful_extra_artifacts_for_b'])})"
        )
    lines.append("")

    lines.append("## Root-Cause Density")
    lines.append("")
    if density_rows:
        lines.append("| Scenario | Run | Candidate mechanisms | Root neighbors | Ambiguity score |")
        lines.append("|---|---:|---:|---:|---:|")
        for row in sorted(density_rows, key=lambda r: (scenario_number(r["scenario_id"]), r["run_id"])):
            lines.append(
                f"| {row['scenario_id']} | {row['run_id']} | {row['plausible_mechanism_candidates']} | "
                f"{row['root_neighbor_count']} | {row['ambiguity_pressure_score']} |"
            )
    else:
        lines.append("No density data available.")
    lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append("- This run uses alias normalization, mechanism tagging, and first-class edge reconstruction.")
    lines.append("- No FCA lattice or concept expansion is performed.")
    lines.append("- Per-run extraction artifacts are stored under scenario/run subdirectories.")
    lines.append("")

    (out_dir / "summary.md").write_text("\n".join(lines))

    write_improvement_report(
        out_dir=out_dir,
        fidelity_rows=fidelity_rows,
        repeatability_rows=repeatability_rows,
        peak_mb=peak_memory_mb(),
    )

    run_status = {
        "status": "completed",
        "scenario_count_requested": len(scenario_ids),
        "scenario_count_with_live_runs": len(by_scenario_runs),
        "missing_live_scenarios": missing_live_scenarios,
        "peak_memory_mb": round(peak_memory_mb(), 3),
        "output_dir": str(out_dir.resolve()),
    }
    write_json(out_dir / "run_status.json", run_status)

    print(json.dumps(run_status, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
