import json
from typing import List

import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from ..models import TestCase, CandidatePair


# Load embedding model once
_embedding_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def value_to_text(value):
    """
    Coerce Jira field values (string, dict, etc.) to a text string.
    Jira Cloud descriptions can be rich text (ADF) -> dicts.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def normalize_issues_to_tests(issues: List[dict]) -> List[TestCase]:
    tests: List[TestCase] = []
    for issue in issues:
        fields = issue.get("fields", {}) or {}
        key = issue.get("key")

        tests.append(
            TestCase(
                key=key,
                summary=fields.get("summary"),
                description=fields.get("description"),
                created=fields.get("created") or "",
                labels=fields.get("labels") or [],
                components=[c.get("name") for c in (fields.get("components") or [])],
            )
        )
    return tests


def build_text_representation(test: TestCase) -> str:
    summary = value_to_text(test.summary).strip()
    description = value_to_text(test.description).strip()

    if description and description != summary:
        return f"Summary: {summary}\nDescription: {description}"
    return f"Summary: {summary}"


def compute_candidates(
    tests: List[TestCase],
    threshold: float,
) -> List[CandidatePair]:
    texts = [build_text_representation(t) for t in tests]

    embeddings = _embedding_model.encode(
        texts,
        batch_size=64,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    embeddings = np.array(embeddings)

    n = len(tests)
    candidate_pairs: List[CandidatePair] = []

    chunk_size = 500
    for i in range(n):
        vec_i = embeddings[i : i + 1]
        start_j = i + 1
        if start_j >= n:
            continue

        for j_start in range(start_j, n, chunk_size):
            j_end = min(j_start + chunk_size, n)
            block = embeddings[j_start:j_end]
            sims = cosine_similarity(vec_i, block)[0]

            for offset, sim in enumerate(sims):
                if sim >= threshold:
                    j = j_start + offset
                    t_i = tests[i]
                    t_j = tests[j]

                    candidate_pairs.append(
                        CandidatePair(
                            issue_key_1=t_i.key,
                            issue_key_2=t_j.key,
                            similarity=float(sim),
                            summary_1=value_to_text(t_i.summary).strip(),
                            summary_2=value_to_text(t_j.summary).strip(),
                        )
                    )

    candidate_pairs.sort(key=lambda c: c.similarity, reverse=True)
    return candidate_pairs