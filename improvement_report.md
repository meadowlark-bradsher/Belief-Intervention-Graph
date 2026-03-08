# Semantic Extraction Improvement Report

This repository-level report summarizes the latest bounded-memory live evaluation run.

- Full detailed report: `out/itbench/live_eval/improvement_report.md`
- Edge reconstruction example: `out/itbench/live_eval/scenario_34_edge_reconstruction_example.md`
- Run status and memory: `out/itbench/live_eval/run_status.json`

## Before vs After

| Metric | Before | After |
|---|---:|---:|
| Root-cause match (true runs) | 0/5 | 2/5 |
| Mean propagation overlap F1 | 0.000 | 0.269 |
| scenario_34 stable core edges | 0 | 7 |
| scenario_34 mean edge Jaccard | 0.000 | 0.157 |

## Runtime

- Peak RSS: **18.33 MB**
- Bounded-memory behavior preserved
- No FCA lattice or concept materialization performed
