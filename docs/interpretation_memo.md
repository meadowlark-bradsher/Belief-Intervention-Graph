# Interpretation Memo: Live Artifact Graph Extraction Upgrade

## Summary

We improved the live ITBench artifact-to-graph extraction layer so that live trajectories now yield a partially stable, canonicalized, mechanism-tagged causal graph rather than mostly noisy, run-specific structure.

This was done without FCA lattice generation and with bounded memory usage.

Evaluation context:
The evaluated runs were non–Chaos Mesh live runs for scenario_16 and scenario_34 (chaos_mesh_enabled: false). Metrics were computed over 5 evaluated runs:

- scenario_16 run 2
- scenario_16 run 3
- scenario_34 run 1
- scenario_34 run 2
- scenario_34 run 3

The pipeline is now prepared for Chaos Mesh–backed runs, which will be evaluated next.

## What changed

### 1. Alias normalization

We added canonicalization for equivalent entities across Kubernetes abstraction levels and live object names.

Examples:

- valkey-pod-1
- valkey-service-1
- otel-demo/Pod/valkey-cart-xyz

are now resolved to a shared canonical component such as:

- valkey

This reduced node fragmentation and improved root-cause comparison against ITBench ground truth.

### 2. Mechanism extraction

We added rules-based mechanism tagging from live artifacts, including tags such as:

- auth_failure
- oom_killed
- timeout
- connection_refused
- probe_failure
- http_abort_or_reset

This allowed the graph to represent not just affected components, but plausible failure mechanisms.

### 3. First-class edge reconstruction

We introduced a propagation edge builder that reconstructs typed edge candidates such as:

- propagates_to
- depends_on
- exhibits_mechanism

This moved the live graph closer to a usable causal representation rather than a flat bag of facts.

## Measured improvement

Before → After:

- Root-cause match: 0/5 → 2/5
- Mean propagation overlap F1: 0.000 → 0.269
- scenario_34 stable core edges: 0 → 7
- scenario_34 mean edge Jaccard: 0.000 → 0.157
- Peak RSS: remained bounded at 18.33 MB

Definition note:
“Stable core edges” are defined as edges appearing in at least two runs, not necessarily in all runs.

## Interpretation

These results show that the extraction layer is now recovering a persistent causal skeleton from live runs, especially for scenario_34, where repeated runs now share stable edges.

This is important because it means the live artifact graph is no longer just execution debris. It is beginning to capture structure that is reusable across runs and comparable to benchmark ground truth.

The system is still not fully reliable:

- root-cause localization is incomplete
- propagation overlap is improved but not yet high
- only scenario_16 and scenario_34 had available live trajectories

But the upgrade demonstrates that the bottleneck has shifted:

- it is no longer memory
- it is no longer total extraction failure
- it is now about coverage and further semantic refinement

## Current conclusion

The live graph pipeline is now viable enough to support continued research.

Specifically:

- ITBench ground truth remains the clean explanatory target
- live trajectories now produce a graph that is noisy but meaningfully recoverable
- this supports the broader goal of studying how agents reason from messy live artifacts toward cleaner causal explanations

## Immediate next step

Collect or generate live trajectories for the missing scenarios:

- scenario_27
- scenario_40
- scenario_41

Then rerun the improved evaluator to test:

- Chaos Mesh–backed scenarios
- discriminability improvements
- whether stable core extraction generalizes beyond scenario_34

## Strategic significance

This upgrade is the first concrete evidence that live chaos artifacts can be transformed into a graph suitable for uncertainty-aware agent reasoning, rather than only benchmark scoring.

The benchmark provides the clean answer.
The live artifact graph now approximates the messy reasoning substrate an agent would face.
