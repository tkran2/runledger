"""Evaluate recorded retrieval outputs. No hidden model calls or fabricated model scores."""
import hashlib
import json
import math

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)

def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()

def string(value, label, maximum=4096):
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{label}: nonempty string of at most {maximum} characters required")

def ids(value, label, maximum=10000, allow_empty=True):
    if not isinstance(value, list) or len(value) > maximum or (not allow_empty and not value):
        raise ValueError(f"{label}: bounded list required")
    for entry in value: string(entry, label, 128)
    if len(set(value)) != len(value): raise ValueError(f"{label}: duplicate IDs")

def validate(payload):
    if not isinstance(payload, dict) or payload.get("type") != "retrieval_evaluation":
        raise ValueError("type must be retrieval_evaluation")
    allowed = {"type", "name", "provenance", "suite", "corpus_ids", "queries", "work_ms"}
    if set(payload) - allowed: raise ValueError("unknown payload fields")
    string(payload.get("name"), "name", 128); string(payload.get("suite"), "suite", 128)
    if payload.get("provenance") not in ("demo", "user_recorded"):
        raise ValueError("provenance must be demo or user_recorded")
    ids(payload.get("corpus_ids"), "corpus_ids", allow_empty=False)
    corpus = set(payload["corpus_ids"])
    work = payload.get("work_ms", 0)
    if type(work) is not int or not 0 <= work <= 5000: raise ValueError("work_ms must be an integer between 0 and 5000")
    queries = payload.get("queries")
    if not isinstance(queries, list) or not 1 <= len(queries) <= 1000:
        raise ValueError("provide 1-1000 recorded queries")
    seen = set()
    for query in queries:
        if not isinstance(query, dict) or set(query) - {"id", "question", "relevant_ids", "retrieved_ids", "citations", "observed_latency_ms"}:
            raise ValueError("invalid query schema")
        string(query.get("id"), "query id", 128); string(query.get("question"), "question")
        if query["id"] in seen: raise ValueError("duplicate query id")
        seen.add(query["id"])
        ids(query.get("relevant_ids"), "relevant_ids", allow_empty=False)
        ids(query.get("retrieved_ids"), "retrieved_ids", maximum=100)
        # Repeated citations count as repeated references, so duplicates are permitted here.
        if not isinstance(query.get("citations"), list) or len(query["citations"]) > 100:
            raise ValueError("citations: bounded list required")
        for citation in query["citations"]: string(citation, "citation", 128)
        if not set(query["relevant_ids"]) <= corpus: raise ValueError("reference labels must exist in the corpus")
        if "observed_latency_ms" in query:
            value = query["observed_latency_ms"]
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                raise ValueError("observed_latency_ms must be finite and nonnegative")
    canonical(payload)  # reject NaN/infinity in any remaining position

def benchmark_fingerprint(payload):
    return digest({"suite": payload["suite"], "corpus_ids": sorted(payload["corpus_ids"]),
                   "references": sorted([{"id": q["id"], "question": q["question"], "relevant_ids": sorted(q["relevant_ids"])}
                                          for q in payload["queries"]], key=lambda q: q["id"])})

def evaluate(payload):
    validate(payload)
    corpus = set(payload["corpus_ids"])
    outcomes, citation_count, invalid_count = [], 0, 0
    for query in payload["queries"]:
        relevant = set(query["relevant_ids"])
        retrieved = query["retrieved_ids"]
        hit = bool(relevant.intersection(retrieved[:3]))
        reciprocal = next((1 / (i + 1) for i, item in enumerate(retrieved) if item in relevant), 0.)
        invalid = [item for item in query["citations"] if item not in corpus]
        citation_count += len(query["citations"]); invalid_count += len(invalid)
        outcomes.append({"id": query["id"], "question": query["question"], "hit_at_3": int(hit),
                         "recall_at_3": len(relevant.intersection(retrieved[:3])) / len(relevant),
                         "reciprocal_rank": reciprocal, "invalid_citations": invalid,
                         "unknown_retrieved_ids": [item for item in retrieved if item not in corpus],
                         "relevant_ids": sorted(relevant), "retrieved_ids": retrieved})
    n = len(outcomes)
    observed_latencies = [q["observed_latency_ms"] for q in payload["queries"] if "observed_latency_ms" in q]
    return {"name": payload["name"], "provenance": payload["provenance"], "suite": payload["suite"],
            "benchmark_sha256": benchmark_fingerprint(payload), "query_count": n,
            "metrics": {"hit_at_3": sum(q["hit_at_3"] for q in outcomes) / n,
                        "recall_at_3": sum(q["recall_at_3"] for q in outcomes) / n,
                        "mrr": sum(q["reciprocal_rank"] for q in outcomes) / n,
                        "invalid_citation_rate": invalid_count / citation_count if citation_count else None,
                        "citation_count": citation_count,
                        "mean_observed_model_latency_ms": sum(observed_latencies) / len(observed_latencies) if observed_latencies else None},
            "per_query": outcomes,
            "limits": ["Recorded-output replay, not a live model benchmark.",
                       "Citation ID existence does not establish factual grounding or entailment.",
                       "Demo fixtures are illustrative, not evidence of real retrieval quality."]}

def compare(baseline, candidate, mrr_tolerance=.05, citation_tolerance=.01):
    if baseline["benchmark_sha256"] != candidate["benchmark_sha256"] or baseline["provenance"] != candidate["provenance"]:
        raise ValueError("comparison requires the same references, corpus, suite and provenance")
    before, after = baseline["metrics"], candidate["metrics"]
    deltas = {key: after[key] - before[key] for key in ("hit_at_3", "recall_at_3", "mrr")}
    failures = []
    if deltas["mrr"] < -mrr_tolerance: failures.append("MRR regression exceeds tolerance")
    if before["invalid_citation_rate"] is None or after["invalid_citation_rate"] is None:
        failures.append("citation rate gate unavailable: at least one run has no citations")
    elif after["invalid_citation_rate"] - before["invalid_citation_rate"] > citation_tolerance:
        failures.append("invalid citation rate increased beyond tolerance")
    return {"passed": not failures, "deltas": deltas, "failures": failures,
            "thresholds": {"max_mrr_drop": mrr_tolerance, "max_invalid_citation_rate_increase": citation_tolerance},
            "benchmark_sha256": baseline["benchmark_sha256"],
            "note": "Deterministic regression guard, not a statistical significance test."}
