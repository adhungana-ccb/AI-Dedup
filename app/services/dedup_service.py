import json
import datetime
from typing import List, Dict, Any, Tuple
from collections import defaultdict, deque

import requests

from ..models import TestCase, CandidatePair


def value_to_text(value: Any) -> str:
    """
    Coerce Jira field values (string, dict, etc.) to plain text.
    Jira Cloud descriptions can be rich text (ADF) => dict/list.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def norm_text(s: str) -> str:
    """
    Normalize text for exact-duplicate comparison:
    - lowercased
    - strip leading/trailing whitespace
    - collapse internal whitespace
    """
    if s is None:
        return ""
    s = s.strip().lower()
    parts = s.split()
    return " ".join(parts)


def parse_created_ts(s: str):
    """
    Parse Jira 'created' timestamp.
    Example formats:
      - 2024-01-02T13:45:36.123+0000
      - 2024-01-02T13:45:36.123Z
    We'll be defensive and fall back if parsing fails.
    """
    if not s:
        return None

    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.datetime.strptime(s, fmt)
        except Exception:
            continue

    try:
        base = s.split(".")[0]
        return datetime.datetime.strptime(base, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def choose_canonical(tests: List[TestCase]) -> TestCase:
    """
    Given a list of tests that are exact duplicates (same normalized
    summary + description), choose the canonical one:
    - Prefer test with latest 'created' timestamp.
    - If timestamps can't be parsed, fall back to max key.
    """
    best = None
    best_dt = None

    for t in tests:
        created = t.created or ""
        dt = parse_created_ts(created)
        if dt is not None:
            if best_dt is None or dt > best_dt:
                best_dt = dt
                best = t

    if best is not None:
        return best

    # Fallback: choose test with 'max' key (usually highest number)
    return max(tests, key=lambda x: x.key or "")


def dedup_exact_duplicates(tests: List[TestCase]) -> List[Dict[str, Any]]:
    """
    Perform exact-duplicate deduplication across all tests.

    Group tests by (normalized summary, normalized description_text).
    For each group with size > 1:
      - choose a canonical test (latest created),
      - mark other tests as duplicates of that canonical.

    Returns a list of decisions:
      {
        "group_id": str,
        "canonical_key": str,
        "duplicate_key": str,
        "reason": str
      }
    """
    grouped: Dict[Tuple[str, str], List[TestCase]] = {}
    for t in tests:
        summary_text = value_to_text(t.summary)
        desc_text = value_to_text(t.description)
        key = (norm_text(summary_text), norm_text(desc_text))
        grouped.setdefault(key, []).append(t)

    decisions: List[Dict[str, Any]] = []
    group_idx = 0

    for _, group in grouped.items():
        if len(group) <= 1:
            continue

        group_idx += 1
        group_id = f"exact_group_{group_idx}"
        canonical = choose_canonical(group)
        canonical_key = canonical.key

        for t in group:
            if t.key == canonical_key:
                continue

            decisions.append(
                {
                    "group_id": group_id,
                    "canonical_key": canonical_key,
                    "duplicate_key": t.key,
                    "reason": (
                        "Exact duplicate (same summary and description); "
                        "kept the most recently created test as the main one."
                    ),
                }
            )

    return decisions


def _build_clusters_from_candidates(
    tests: List[TestCase],
    candidates: List[CandidatePair],
) -> List[List[TestCase]]:
    """
    Build connected components (clusters) from candidate pairs.
    Each cluster is a list of TestCase objects.
    """
    test_map = {t.key: t for t in tests}
    adj: Dict[str, set] = defaultdict(set)

    for c in candidates:
        k1 = c.issue_key_1
        k2 = c.issue_key_2
        if k1 in test_map and k2 in test_map:
            adj[k1].add(k2)
            adj[k2].add(k1)

    clusters: List[List[TestCase]] = []
    visited = set()

    for key in adj.keys():
        if key in visited:
            continue
        comp_keys = []
        dq = deque([key])
        visited.add(key)
        while dq:
            cur = dq.popleft()
            comp_keys.append(cur)
            for nei in adj[cur]:
                if nei not in visited:
                    visited.add(nei)
                    dq.append(nei)
        if len(comp_keys) > 1:
            clusters.append([test_map[k] for k in comp_keys])

    return clusters


_iq_default_model: str = ""

# Optional: force a specific model ID to avoid /v1/models variability.
_FORCED_IQ_MODEL_ID: str = ""

# Track total IQ token usage for the current dedup run
_iq_usage_counters = {"prompt": 0, "completion": 0, "total": 0}


def _reset_iq_usage_counters():
    _iq_usage_counters["prompt"] = 0
    _iq_usage_counters["completion"] = 0
    _iq_usage_counters["total"] = 0


def get_iq_usage_totals() -> Dict[str, int]:
    """
    Return the total IQ token usage accumulated during the last dedup_with_iq run.
    """
    return dict(_iq_usage_counters)


def _get_or_fetch_iq_model(iq_base_url: str, iq_token: str) -> str:
    """
    Fetch available models from IQ via /v1/models and cache the first one,
    unless a forced model ID is configured.
    """
    global _iq_default_model

    # Return cached model if we already have one
    if _iq_default_model:
        return _iq_default_model

    # If a forced model ID is configured, use it without calling /v1/models
    if _FORCED_IQ_MODEL_ID:
        _iq_default_model = _FORCED_IQ_MODEL_ID
        print(f"[IQ] Using forced model: {_iq_default_model}")
        return _iq_default_model

    url = iq_base_url.rstrip("/") + "/v1/models"
    headers = {
        "Authorization": f"Bearer {iq_token}",
        "Content-Type": "application/json",
    }

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    models: List[str] = []

    if isinstance(data, dict) and "data" in data:
        for m in data["data"]:
            mid = m.get("id")
            if mid:
                models.append(mid)
    elif isinstance(data, list):
        for m in data:
            mid = m.get("id") if isinstance(m, dict) else None
            if mid:
                models.append(mid)

    if not models:
        print("[IQ] /v1/models returned no model IDs:", data)
        raise RuntimeError("IQ /v1/models did not return any model IDs.")

    print("[IQ debug] models from /v1/models:", models)
    _iq_default_model = models[0]
    print(f"[IQ] Using model: {_iq_default_model}")
    return _iq_default_model


def _extract_json_from_content(content: str) -> str:
    """
    Try to extract a valid JSON string from model output.

    The IQ model should return raw JSON, but in practice it sometimes wraps it
    in fenced code blocks or includes extra commentary.

    This helper strips common wrappers and returns what looks like the JSON
    object text so that json.loads can succeed.
    """
    if not content:
        return content

    text = content.strip()

    # Remove a leading and trailing "fence" line (for example, a line that starts with 3 backticks)
    fence = "`" * 3
    if text.startswith(fence):
        lines = text.splitlines()
        if len(lines) >= 2:
            # Drop the first line (opening fence)
            lines = lines[1:]
        if lines and lines[-1].strip().startswith(fence):
            # Drop the last line (closing fence)
            lines = lines[:-1]
        text = "\n".join(lines).strip()

    # Fallback: grab content between first '{' and last '}'
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]

    return text


def _call_iq_for_cluster(
    cluster_tests: List[TestCase],
    instructions: str,
    iq_base_url: str,
    iq_token: str,
    model: str,
) -> Dict[str, Any]:
    """
    Call IQ LLM for a single cluster.

    This assumes an OpenAI-chat-like API at:
      POST {iq_base_url}/v1/chat/completions
    """
    tests_payload = []
    for t in cluster_tests:
        tests_payload.append(
            {
                "key": t.key,
                "summary": value_to_text(t.summary),
                "description": value_to_text(t.description),
                "created": t.created,
                "labels": t.labels,
                "components": t.components,
            }
        )

    system_prompt = (
        "You are a QA test deduplication assistant.\n"
        "\n"
        "Input format:\n"
        "- You receive JSON with two top-level keys:\n"
        "  - 'cluster_tests': a list of test cases that are likely duplicates or overlapping.\n"
        "  - 'instructions': optional additional guidance from the user.\n"
        "\n"
        "Follow this order of priority:\n"
        "1) Always obey the required JSON output schema.\n"
        "2) Obey any explicit guidance in 'instructions' when it does not conflict with the schema.\n"
        "3) Otherwise, apply the base rules below.\n"
        "\n"
        "Base rules:\n"
        "- Only merge tests if they represent the same scenario or can be parameterized "
        "  without losing coverage.\n"
        "- Do NOT merge tests that differ by important variations like 'restore to original' "
        "  vs 'restore to alternate', or 'export to local' vs 'export to azure'.\n"
        "- Prefer merging tests that differ only by trivial wording or parameter values "
        "  (for example, different folders of the same type, or equivalent visibility types "
        "  that do not change behavior).\n"
        "- If multiple tests are clearly redundant, you may use metadata such as labels or "
        "  created timestamps to pick a canonical (for example, preferring automated tests "
        "  over purely manual duplicates if coverage is the same).\n"
        "- For merged tests, pick canonical keys and classify other tests as duplicates.\n"
        "- For tests that should remain separate, mark them as 'keep'.\n"
        "- Always return STRICT JSON, no extra commentary.\n"
        "- The JSON schema MUST be:\n"
        "  {\n"
        '    "cluster_id": "string",\n'
        '    "decisions": [\n'
        "      {\n"
        '        "key": "string",\n'
        '        "role": "canonical" | "duplicate" | "keep",\n'
        '        "canonical_of": "string or null",\n'
        '        "notes": "string"\n'
        "      }\n"
        "    ]\n"
        "  }\n"
    )

    user_instructions = instructions or "Use your best judgment based on the tests."

    user_payload = {
        "cluster_tests": tests_payload,
        "instructions": user_instructions,
    }

    url = iq_base_url.rstrip("/") + "/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {iq_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(user_payload, ensure_ascii=False),
            },
        ],
        "temperature": 0.0,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    if not resp.ok:
        try:
            err_json = resp.json()
        except Exception:
            err_json = resp.text
        print("=== IQ API ERROR (HTTP) ===")
        print(f"Status: {resp.status_code}")
        print(f"URL: {url}")
        print("Response body:", err_json)
        print("=== END IQ API ERROR (HTTP) ===")
        raise RuntimeError(
            f"IQ API error {resp.status_code} at {url}: {err_json}"
        )

    data = resp.json()

    # Log token usage and accumulate totals if the API returns usage information
    try:
        usage = data.get("usage") or {}
        prompt_tokens = usage.get("prompt_tokens") or 0
        completion_tokens = usage.get("completion_tokens") or 0
        total_tokens = usage.get("total_tokens") or (prompt_tokens + completion_tokens)
        print(
            "[IQ tokens] cluster_call "
            f"prompt={prompt_tokens}, completion={completion_tokens}, total={total_tokens}"
        )
        _iq_usage_counters["prompt"] += prompt_tokens
        _iq_usage_counters["completion"] += completion_tokens
        _iq_usage_counters["total"] += total_tokens
    except Exception:
        # Don't break the flow if usage is missing or in an unexpected format
        pass

    try:
        content = data["choices"][0]["message"]["content"]
    except Exception as e:
        print("=== IQ RESPONSE (unexpected structure) ===")
        try:
            print(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception:
            print(data)
        print("=== END IQ RESPONSE ===")
        raise RuntimeError("Unexpected IQ response format") from e

    # First try to parse the content as-is. If that fails, try to clean it up.
    try:
        result = json.loads(content)
    except Exception as e:
        cleaned = _extract_json_from_content(content or "")
        try:
            result = json.loads(cleaned)
        except Exception:
            print("=== IQ CONTENT (not valid JSON) ===")
            print(content)
            print("=== CLEANED IQ CONTENT ATTEMPT ===")
            print(cleaned)
            print("=== END IQ CONTENT ===")
            raise RuntimeError("AI did not return valid JSON") from e

    return result


def dedup_with_iq(
    tests: List[TestCase],
    candidates: List[CandidatePair],
    instructions: str,
    iq_base_url: str,
    iq_token: str,
    max_clusters: int = 50,
) -> List[Dict[str, Any]]:
    """
    IQ-backed deduplication with fallback.
    """
    _reset_iq_usage_counters()  # reset totals for this run

    clusters = _build_clusters_from_candidates(tests, candidates)
    print(f"[IQ debug] tests: {len(tests)}")
    print(f"[IQ debug] candidates: {len(candidates)}")
    print(f"[IQ debug] clusters: {len(clusters)}")

    decisions: List[Dict[str, Any]] = []

    clusters_to_process = clusters[:max_clusters]
    print(
        f"[IQ debug] clusters_to_process (max {max_clusters}): "
        f"{len(clusters_to_process)}"
    )

    if not iq_token:
        print("[IQ] No IQ token provided; falling back to exact duplicate detection only.")
        return dedup_exact_duplicates(tests)

    try:
        model_name = _get_or_fetch_iq_model(iq_base_url, iq_token)
    except Exception as e:
        print(f"[IQ] Failed to fetch models, falling back to exact duplicates: {e}")
        return dedup_exact_duplicates(tests)

    for idx, cluster_tests in enumerate(clusters_to_process, start=1):
        cluster_id = f"iq_cluster_{idx}"
        print(
            f"[IQ debug] Processing cluster {cluster_id} with "
            f"{len(cluster_tests)} tests"
        )

        try:
            iq_result = _call_iq_for_cluster(
                cluster_tests=cluster_tests,
                instructions=instructions,
                iq_base_url=iq_base_url,
                iq_token=iq_token,
                model=model_name,
            )
        except Exception as e:
            print(f"[IQ cluster {cluster_id} error] {e}")
            cluster_decisions = dedup_exact_duplicates(cluster_tests)
            for d in cluster_decisions:
                # Indicate fallback in group_id (for debugging), but do not pollute the reason text.
                d["group_id"] = f"{cluster_id}_fallback"
            decisions.extend(cluster_decisions)
            continue

        iq_cluster_id = iq_result.get("cluster_id", cluster_id)
        for d in iq_result.get("decisions", []):
            role = d.get("role")
            if role != "duplicate":
                continue
            canonical_of = d.get("canonical_of")
            key = d.get("key")
            notes = d.get("notes") or "Suggested by AI."

            if not canonical_of or not key:
                continue

            decisions.append(
                {
                    "group_id": iq_cluster_id,
                    "canonical_key": canonical_of,
                    "duplicate_key": key,
                    "reason": notes,
                }
            )

    return decisions


def _clean_fact_text(raw: str) -> str:
    """
    Clean up fact text:
    - Strip leading/trailing spaces.
    - Remove leading 'Did you know' if present.
    """
    if not raw:
        return ""

    fact = raw.strip()

    lower = fact.lower()
    if lower.startswith("did you know"):
        # Remove up to the first '?' if it exists, else remove the phrase
        qpos = fact.find("?")
        if qpos != -1:
            fact = fact[qpos + 1 :].lstrip(" .-–—")
        else:
            fact = fact[len("Did you know") :].lstrip(" .-–—")

    return fact.strip()


def _contains_testing_terms(text: str) -> bool:
    """
    Check if text mentions software testing / QA.
    """
    lower = text.lower()
    keywords = [
        "software testing",
        "test automation",
        "qa ",
        " qa",
        "quality assurance",
        "test scripts",
        "test cases",
        "testing tools",
    ]
    return any(kw in lower for kw in keywords)


def fetch_ai_fact(iq_base_url: str, iq_token: str) -> str:
    """
    Use IQ to generate a short 'Did you know?' fact about impactful AI developments
    OUTSIDE of software testing/QA.

    If IQ is not available or fails, returns a curated, high-impact AI fact
    (never about software testing).
    """
    fallback_facts = [
        (
            "DeepMind's AlphaFold predicted the 3D structure of over 200 million proteins—"
            "essentially every protein known to science—transforming how researchers "
            "approach drug discovery and disease research."
        ),
        (
            "Researchers recently used AI to discover a new antibiotic that kills a "
            "drug-resistant superbug by virtually screening hundreds of millions of "
            "molecules—something that would have been impractical by hand."
        ),
        (
            "The European Union agreed on the AI Act, the first comprehensive law to "
            "regulate AI systems, in a way similar to how GDPR reshaped global data privacy."
        ),
        (
            "Modern AI systems can translate between dozens of languages in real time, "
            "enabling cross-border collaboration that would have required human interpreters "
            "for every conversation just a decade ago."
        ),
        (
            "AI models have been used to design entirely new materials and proteins that do "
            "not exist in nature, opening up possibilities for new vaccines and sustainable materials."
        ),
        (
            "AI is being used in agriculture to analyze satellite and drone imagery, helping "
            "farmers optimize water usage and increase yields while reducing environmental impact."
        ),
        (
            "Some logistics companies use AI route-optimization engines that reduce fuel "
            "consumption and CO₂ emissions by double-digit percentages across massive fleets."
        ),
        (
            "Financial institutions deploy AI systems that monitor billions of transactions "
            "in real time, catching fraud patterns that would be impossible for humans to spot."
        ),
        (
            "Hospitals are piloting AI tools that read X-rays and CT scans as accurately as "
            "specialist radiologists for specific tasks, acting as a second set of eyes in diagnosis."
        ),
        (
            "AI-generated deepfakes have become so realistic that entire research fields are now "
            "dedicated to detecting synthetic media and proving that content is authentic."
        ),
    ]

    # If no IQ token, just return one curated fact
    import random

    if not iq_token:
        return random.choice(fallback_facts)

    try:
        model_name = _get_or_fetch_iq_model(iq_base_url, iq_token)
    except Exception as e:
        print(f"[IQ fact] Failed to fetch models: {e}")
        return random.choice(fallback_facts)

    system_prompt = (
        "You are an assistant that shares one short 'Did you know?' fact about impactful "
        "AI developments in the real world. Focus on concrete, recent examples (last ~5–7 years) "
        "in areas like science, medicine, logistics, climate, finance, media, or regulation.\n"
        "- The fact should be 1–2 sentences, surprising but accurate.\n"
        "- Do NOT talk about software testing, QA, test automation, or test tools.\n"
        "- Do NOT include the words 'Did you know' in your answer; just output the fact itself.\n"
    )

    url = iq_base_url.rstrip("/") + "/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {iq_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "Share one 'Did you know?' fact."},
        ],
        "temperature": 0.6,
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if not resp.ok:
            try:
                err_json = resp.json()
            except Exception:
                err_json = resp.text
            print("[IQ fact] HTTP error:", resp.status_code, err_json)
            return random.choice(fallback_facts)

        data = resp.json()

        # Log token usage for facts (not accumulated in dedup counters)
        try:
            usage = data.get("usage") or {}
            prompt_tokens = usage.get("prompt_tokens")
            completion_tokens = usage.get("completion_tokens")
            total_tokens = usage.get("total_tokens")
            print(
                "[IQ tokens][fact] "
                f"prompt={prompt_tokens}, completion={completion_tokens}, total={total_tokens}"
            )
        except Exception:
            pass

        try:
            content = data["choices"][0]["message"]["content"]
        except Exception as e:
            print("[IQ fact] Unexpected response structure:", data)
            return random.choice(fallback_facts)

        raw_fact = (content or "").strip()
        fact = _clean_fact_text(raw_fact)

        # Filter out anything that accidentally mentions testing/QA
        if _contains_testing_terms(fact):
            print("[IQ fact] Filtered out testing-related fact:", fact)
            return random.choice(fallback_facts)

        if not fact:
            return random.choice(fallback_facts)

        return fact
    except Exception as e:
        print("[IQ fact] Exception calling IQ:", e)
        return random.choice(fallback_facts)