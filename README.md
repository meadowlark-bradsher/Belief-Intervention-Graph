# hypothesis-graph

Canonical commands:

- `make build`
- `make experiment`
- `make all`
- `make smoke`
- `make test`
- `make oracle_seed`
- `make oracle_validate`
- `make hcg_snapshot`
- `make oracle_seed_real` (preflight first; requires `CONFIRM=1` to execute)
- `make oracle_validate_real`
- `make hcg_snapshot_real`
- `make oracle_status_real`

Real profile safety rails:

- Dry-run preflight is always executed first for `oracle_seed_real` / `oracle_expand_real`.
- Run execution requires `CONFIRM=1`.
- Hard pair cap defaults to `MAX_PAIRS=300000`; override with `ALLOW_LARGE=1`.
- Scale knobs: `PRED_N`, `SECRET_N`, `PART_SIZE`, `MAX_PAIRS`.
- Real defaults: `REAL_WITNESS_TYPE=hash`, `REAL_PREDCLASS=../../bridge-rl-portfolio-private/oracle_predclass/out/run_006/classified.jsonl`, `REAL_SECRETS=data/real/secrets_158.jsonl`.

Detailed module docs are in `/Users/meadowlarkbradsher/workspace/repos/genai/cmbs-hub/hypothesis-graph/hypothesis_graph/README.md`.

By default, these commands run against `/Users/meadowlarkbradsher/workspace/repos/genai/cmbs-hub/hypothesis-graph/data/fixtures` and write outputs to `/Users/meadowlarkbradsher/workspace/repos/genai/cmbs-hub/hypothesis-graph/out/fixture_run`.
