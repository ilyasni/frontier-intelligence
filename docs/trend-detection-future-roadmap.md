# Trend Detection Future Roadmap

Status: saved for later implementation, not scheduled yet.

## Purpose

This note captures promising 2024-2025 trend-detection methods that were reviewed
against the current Frontier Intelligence architecture.

Current baseline in this repo:

- semantic dedupe and semantic clusters
- stable trends and emerging signals
- temporal metrics and change-point detection
- source scoring and cross-source quality signals
- Qdrant + Neo4j + MCP/admin observability

The goal of this note is not to list every research direction, but to record
what is most likely to improve the existing production pipeline with acceptable
cost and operational risk.

## External Methods Reviewed

### 1. BERTopic + Topics Over Time + Topic Emergence Maps

Primary sources:

- https://bertopic.org/
- https://maartengr.github.io/BERTopic/getting_started/topicsovertime/topicsovertime.html
- https://www.mdpi.com/3248222

Assessment:

- Best next-step candidate for this project.
- Fits the current batch architecture.
- Improves explainability and weak-signal mapping more than it improves core
  ingestion.

Why it fits:

- Can run on top of existing semantic clusters instead of replacing them.
- Gives topic-level labels and topic trajectories over time.
- Makes it possible to build Topic Emergence Maps (TEM) with quadrants like
  weak / strong / latent / declining.

Pros:

- Strong explainability.
- Works well with the current semantic-cluster-first approach.
- Helps weak-signal analysis without requiring a streaming rewrite.
- Produces better labels than the current heuristic-only cluster titles.

Cons:

- Additional CPU/RAM pressure.
- Topic identifiers may drift between runs.
- Requires tuning of UMAP/HDBSCAN and topic reduction.

Recommendation:

- Implement as a separate batch layer after semantic clustering.
- Start with one workspace, ideally `disruption`.

### 2. GraphRAG

Primary sources:

- https://www.microsoft.com/en-us/research/project/graphrag/%3Flang%3Dzh-cn
- https://www.microsoft.com/en-us/research/blog/moving-to-graphrag-1-0-streamlining-ergonomics-for-developers-and-users/

Assessment:

- Good fit, but not as a detector.
- Best used as a reasoning layer on top of existing trends, signals, concepts,
  and entity relations.

Why it fits:

- The repo already uses Neo4j and graph-linked concepts.
- GraphRAG can improve cluster explanation and multi-hop synthesis in MCP/admin.

Pros:

- Better analyst-facing explanations.
- Better multi-hop reasoning across entities and clusters.
- Can complement vector search without changing the detection pipeline.

Cons:

- Does not directly improve trend detection quality.
- Adds complexity and latency to reasoning paths.

Recommendation:

- Treat as a later synthesis layer, not as a replacement for current detection.

### 3. Temporal Knowledge Graph Forecasting (GenTKG / RAG + tKG)

Primary source:

- https://aclanthology.org/2024.findings-naacl.268/

Assessment:

- Promising for future forecasting, but not the next implementation step.

Why it fits partially:

- The project already stores concepts and graph relations.
- Long-term, this could support "what may happen next" style forecasting.

Pros:

- Useful for event forecasting and temporal reasoning.
- Can build on top of graph history once the temporal graph is mature enough.

Cons:

- Research-heavy.
- Requires better temporal fact modeling than the current graph layer exposes.
- Hard to validate without a dedicated forecasting benchmark.

Recommendation:

- Revisit after topic modeling and cross-source fusion are stable.

### 4. Cross-Source Fusion (Kalman / Bayesian / Dempster-Shafer)

Assessment:

- Worth considering after topic modeling.
- More realistic for this system than jumping straight into dynamic GNNs.

Why it fits:

- The platform already treats sources differently via source score,
  authority, runtime health, and relevance quality.
- These methods can fuse noisy cross-platform evidence into a more stable
  emergence probability.

Pros:

- Good for multi-platform weak-signal monitoring.
- Can reduce sensitivity to single-source spikes.
- More interpretable and cheaper than deep graph architectures.

Cons:

- Requires careful definition of observation streams and priors.
- Easy to overcomplicate mathematically without strong practical payoff.

Recommendation:

- Start simple.
- Prefer Bayesian or Kalman-style smoothing before Dempster-Shafer.

### 5. Temporal GNN / TGN / Meta-learning GNN

Reference examples reviewed:

- https://www.ijcai.org/proceedings/2025/322
- https://proceedings.mlr.press/v267/li25ci.html
- https://www.nature.com/articles/s41598-026-35385-w

Assessment:

- Not a near-term fit.
- Strong research direction, weak production fit for the current stack.

Why it does not fit now:

- The current bottleneck is not lack of graph neural modeling.
- The current bottleneck is cluster quality, topic explainability,
  source structure, and signal fusion.

Pros:

- Potentially strong for diffusion phase modeling and event ordering.
- Good long-term research direction if graph forecasting becomes core.

Cons:

- Expensive to implement and validate.
- Requires better graph datasets and clearer supervised targets.
- Overkill for the current production maturity.

Recommendation:

- Do not prioritize now.

### 6. CasFT / Neural ODE for popularity trajectory prediction

Assessment:

- Interesting research direction, low priority for this project.

Pros:

- Could forecast full popularity curves, not only a local burst.
- Potential value for viral-content trajectory prediction.

Cons:

- Too far from the current main bottleneck.
- Research-heavy and expensive to validate.

Recommendation:

- Not a practical next step.

### 7. Kafka + Flink Streaming

Primary source reviewed:

- https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/guarantees/

Assessment:

- Correct architecture for true real-time analytics.
- Not justified yet for this project.

Pros:

- Strong stateful streaming.
- Sliding windows and exactly-once semantics.
- Good for sub-second online trend detection.

Cons:

- Major infrastructure jump.
- Current system problems are not primarily latency-related.

Recommendation:

- Keep as a future scaling path, not a current roadmap item.

## Overall Priority

### Recommended next wave

1. BERTopic on top of semantic clusters
2. Topics over time
3. Topic Emergence Maps (TEM)
4. Lightweight cross-source fusion
5. GraphRAG for reasoning/synthesis

### Deliberately postponed

1. Temporal GNN / TGN
2. CasFT / Neural ODE
3. Full Kafka/Flink streaming migration
4. Heavy temporal KG forecasting

## Proposed Future Implementation Order

### Phase 1: Topic layer

- Add a batch job for BERTopic over representative semantic-cluster evidence
- Persist topic labels, top terms, topic assignments, and topic time-series

### Phase 2: Weak-signal map

- Build TEM metrics on top of topic time-series
- Add topic quadrants: weak / strong / latent / declining

### Phase 3: Cross-source fusion

- Add a light Bayesian or Kalman-style fusion layer
- Combine source score, source diversity, topic growth, and signal persistence

### Phase 4: Graph-aware reasoning

- Use GraphRAG-style retrieval for analyst workflows and MCP synthesis
- Do not move core detection logic into GraphRAG

## Why This Order

This sequence keeps the system aligned with the current architecture:

- batch/offline analytics instead of live-path complexity
- explanation and weak-signal quality before research-heavy forecasting
- moderate operational risk
- strong fit with the existing semantic clusters, Qdrant, Neo4j, admin, and MCP

## Practical Decision

If and when this work starts, the best first implementation should be:

- BERTopic
- topics over time
- TEM

Everything else should be evaluated only after that layer is running and
measured on real workspaces.
