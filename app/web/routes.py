# app/web/routes.py
import csv
import io
import json

from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
    jsonify,
)

from ..services.jira_service import JiraClient
from ..services.similarity_service import (
    normalize_issues_to_tests,
    compute_candidates,
    value_to_text,
)
from ..services.dedup_service import (
    dedup_exact_duplicates,
    dedup_with_iq,
    fetch_ai_fact,
    get_iq_usage_totals,
)

web_bp = Blueprint("web", __name__, template_folder="templates")

# In-memory storage for last results (simple, single-user)
_last_tests_jsonl_bytes = None
_last_candidates_csv_bytes = None
_last_summary = None
_last_candidates = []      # List[CandidatePair]
_last_tests = []           # List[TestCase]
_last_dedup_decisions = [] # List[dict]
_last_instruction_text = ""

UI_PAGE_SIZE_DEFAULT = 20

DEFAULT_OTHER_LABELS_TEXT = "MABL_TBA_Regression, mabl-automated, CANNOT_MABL"
DEFAULT_STATUS_EXCLUDE = "Rejected"


def build_jql(project, issue_type, labels, status_exclude, search_term, exclude_labels):
    """
    Build a JQL string of the form:

      project = <project>
      AND issuetype = <issue_type>
      [AND labels IN (<labels...>)]
      [AND text ~ "<search_term>"]
      [AND labels NOT IN (<exclude_labels...>)]
      [AND status != <status_exclude>]
    """
    clauses = [f"project = {project}", f"issuetype = {issue_type}"]

    if labels:
        labels_clause = ", ".join(f'"{lbl}"' for lbl in labels)
        clauses.append(f"labels IN ({labels_clause})")

    if search_term:
        search = search_term.replace('"', '\\"')
        clauses.append(f'text ~ "{search}"')

    if exclude_labels:
        exclude_clause = ", ".join(f'"{lbl}"' for lbl in exclude_labels)
        clauses.append(f"labels NOT IN ({exclude_clause})")

    if status_exclude:
        clauses.append(f"status != {status_exclude}")

    return " AND ".join(clauses)


def _flatten_adf_node(node, parts):
    """
    Recursively extract plain text from Jira ADF nodes.
    """
    if isinstance(node, dict):
        if node.get("type") == "text":
            text = node.get("text")
            if text:
                parts.append(text)
        for child in node.get("content", []):
            _flatten_adf_node(child, parts)
    elif isinstance(node, list):
        for child in node:
            _flatten_adf_node(child, parts)


def description_to_plain(value) -> str:
    """
    Convert Jira description to plain text for display/inspection.
    """
    if isinstance(value, (dict, list)):
        parts: list[str] = []
        _flatten_adf_node(value, parts)
        return " ".join(parts).strip()
    return value_to_text(value).strip()


@web_bp.route("/", methods=["GET", "POST"])
def index():
    global _last_tests_jsonl_bytes, _last_candidates_csv_bytes, _last_summary
    global _last_candidates, _last_tests, _last_dedup_decisions, _last_instruction_text

    default_base = current_app.config.get("DEFAULT_JIRA_BASE_URL", "")
    iq_base_url = current_app.config.get("DEFAULT_IQ_BASE_URL", "https://api.iq.cudasvc.com")

    if request.method == "POST":
        action = request.form.get("action", "analyze")

        if action == "analyze":
            # Phase 1 & 2: Fetch from Jira + similarity analysis
            base_url = default_base
            email = request.form.get("email") or ""
            api_token = request.form.get("api_token") or ""
            iq_token = request.form.get("iq_token") or ""
            project = request.form.get("project") or ""
            issue_type = request.form.get("issue_type") or "Test"
            status_excl = DEFAULT_STATUS_EXCLUDE

            # Threshold: whole-number percent (1–99)
            threshold_percent_str = request.form.get("threshold") or "85"
            try:
                threshold_percent = int(threshold_percent_str)
            except ValueError:
                threshold_percent = 85
            if threshold_percent < 1:
                threshold_percent = 1
            if threshold_percent > 99:
                threshold_percent = 99
            threshold = threshold_percent / 100.0

            # Fixed Jira fetch page size; UI page size is separate
            page_size = 20

            search_term = request.form.get("search_term") or ""
            other_labels_raw = request.form.get("other_labels") or ""
            labels = [s.strip() for s in other_labels_raw.split(",") if s.strip()]

            exclude_labels_raw = request.form.get("exclude_labels") or ""
            exclude_labels = [s.strip() for s in exclude_labels_raw.split(",") if s.strip()]

            if not (base_url and email and api_token and project):
                flash("Jira base URL (from config), email, Jira token, and project key are required.", "error")
                return redirect(url_for("web.index"))

            jql = build_jql(
                project=project,
                issue_type=issue_type,
                labels=labels,
                status_exclude=status_excl,
                search_term=search_term,
                exclude_labels=exclude_labels,
            )

            try:
                jira = JiraClient(base_url, email, api_token)
                issues = jira.fetch_issues(jql, page_size=page_size)
            except Exception as e:
                flash(f"Error fetching from Jira: {e}", "error")
                return redirect(url_for("web.index"))

            tests = normalize_issues_to_tests(issues)

            # Store normalized tests as JSONL
            buf_jsonl = io.StringIO()
            for t in tests:
                buf_jsonl.write(json.dumps(t.__dict__, ensure_ascii=False) + "\n")
            _last_tests_jsonl_bytes = buf_jsonl.getvalue().encode("utf-8")

            try:
                candidates = compute_candidates(tests, threshold)
            except Exception as e:
                flash(f"Error during similarity computation: {e}", "error")
                return redirect(url_for("web.index"))

            _last_candidates = candidates
            _last_tests = tests
            _last_dedup_decisions = []
            _last_instruction_text = ""

            # Build CSV for candidates
            buf_csv = io.StringIO()
            writer = csv.writer(buf_csv)
            writer.writerow(
                [
                    "issue_key_1",
                    "issue_key_2",
                    "similarity",
                    "similarity_percent",
                    "summary_1",
                    "summary_2",
                ]
            )
            for c in candidates:
                writer.writerow(
                    [
                        c.issue_key_1,
                        c.issue_key_2,
                        f"{c.similarity:.4f}",
                        f"{c.similarity * 100:.2f}",
                        c.summary_1,
                        c.summary_2,
                    ]
                )
            _last_candidates_csv_bytes = buf_csv.getvalue().encode("utf-8")

            _last_summary = {
                "count_issues": len(issues),
                "count_candidates": len(candidates),
                "threshold": threshold,
                "threshold_percent": threshold_percent,
                "jql": jql,
                "base_url": base_url,
                "project": project,
                "issue_type": issue_type,
                "status_exclude": status_excl,
                "page_size": page_size,
                "search_term": search_term,
                "other_labels_text": other_labels_raw,
                "exclude_labels_text": exclude_labels_raw,
                "email": email,
                "api_token": api_token,
                "iq_token": iq_token,
            }

            return redirect(url_for("web.index"))

        elif action == "dedup":
            instruction_text = request.form.get("instruction_text") or ""
            _last_instruction_text = instruction_text

            if not _last_tests:
                flash("Run Steps 1 and 2 first before analyzing duplicates.", "error")
                return redirect(url_for("web.index"))

            iq_token = ""
            if _last_summary:
                iq_token = _last_summary.get("iq_token", "")

            decisions = []
            tokens_prompt = tokens_completion = tokens_total = 0

            if iq_token:
                try:
                    decisions = dedup_with_iq(
                        tests=_last_tests,
                        candidates=_last_candidates,
                        instructions=instruction_text,
                        iq_base_url=iq_base_url,
                        iq_token=iq_token,
                        max_clusters=15,
                    )
                    if not decisions:
                        decisions = dedup_exact_duplicates(_last_tests)
                        flash(
                            "AI returned no suggestions; showing only tests that are exact duplicates.",
                            "info",
                        )
                    usage_totals = get_iq_usage_totals()
                    tokens_prompt = usage_totals.get("prompt", 0)
                    tokens_completion = usage_totals.get("completion", 0)
                    tokens_total = usage_totals.get("total", 0)
                    print(
                        "[IQ tokens][dedup total] "
                        f"prompt={tokens_prompt}, completion={tokens_completion}, total={tokens_total}"
                    )
                    flash(
                        f"Total IQ tokens used for this dedup run: "
                        f"prompt={tokens_prompt}, completion={tokens_completion}, total={tokens_total}",
                        "info",
                    )
                except Exception:
                    flash(
                        "There was an internal error when using AI to analyze duplicates; "
                        "showing only tests that are exact duplicates.",
                        "error",
                    )
                    decisions = dedup_exact_duplicates(_last_tests)
            else:
                flash(
                    "AI key is not provided; showing only tests that are exact duplicates of each other.",
                    "info",
                )
                decisions = dedup_exact_duplicates(_last_tests)

            # Enrich decisions with summaries and reason_to_merge for display and merging.
            test_map = {t.key: t for t in _last_tests}
            for d in decisions:
                canon = test_map.get(d["canonical_key"])
                dup = test_map.get(d["duplicate_key"])
                if canon:
                    d["canonical_summary"] = value_to_text(canon.summary).strip()
                else:
                    d["canonical_summary"] = ""
                if dup:
                    d["duplicate_summary"] = value_to_text(dup.summary).strip()
                    d["duplicate_description"] = description_to_plain(dup.description)
                else:
                    d["duplicate_summary"] = ""
                    d["duplicate_description"] = ""

                base_reason = d.get("reason", "")
                if d["canonical_summary"]:
                    d["reason_to_merge"] = (
                        f"{base_reason} | Updated canonical summary: {d['canonical_summary']}"
                    )
                else:
                    d["reason_to_merge"] = base_reason

            _last_dedup_decisions = decisions
            flash(
                f"AI identified {len(decisions)} tests that can be treated as duplicates of another test.",
                "info",
            )
            return redirect(url_for("web.index"))

    # GET: render UI

    base_url = default_base
    email = ""
    api_token = ""
    iq_token = ""
    project = "JAN"
    issue_type = "Test"
    threshold = 0.85
    threshold_percent = 85
    page_size = 20
    other_labels_text = DEFAULT_OTHER_LABELS_TEXT
    search_term = ""
    exclude_labels_text = ""

    if _last_summary:
        base_url = _last_summary.get("base_url", base_url)
        project = _last_summary.get("project", project)
        issue_type = _last_summary.get("issue_type", issue_type)
        threshold = _last_summary.get("threshold", threshold)
        threshold_percent = _last_summary.get(
            "threshold_percent", int(round(threshold * 100))
        )
        page_size = _last_summary.get("page_size", page_size)
        other_labels_text = _last_summary.get("other_labels_text", other_labels_text)
        exclude_labels_text = _last_summary.get("exclude_labels_text", exclude_labels_text)
        search_term = _last_summary.get("search_term", search_term)
        email = _last_summary.get("email", email)
        api_token = _last_summary.get("api_token", api_token)
        iq_token = _last_summary.get("iq_token", iq_token)

    page_str = request.args.get("page", "1") or "1"
    ui_page_size_str = (
        request.args.get("ui_page_size", str(UI_PAGE_SIZE_DEFAULT))
        or str(UI_PAGE_SIZE_DEFAULT)
    )

    try:
        page = int(page_str)
    except ValueError:
        page = 1

    try:
        ui_page_size = int(ui_page_size_str)
    except ValueError:
        ui_page_size = UI_PAGE_SIZE_DEFAULT

    if ui_page_size not in (20, 50, 100):
        ui_page_size = UI_PAGE_SIZE_DEFAULT

    total_candidates = len(_last_candidates) if _last_candidates else 0
    total_pages = (
        (total_candidates + ui_page_size - 1) // ui_page_size if total_candidates else 1
    )

    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start_idx = (page - 1) * ui_page_size
    end_idx = start_idx + ui_page_size

    candidates_page = []
    if _last_candidates:
        for c in _last_candidates[start_idx:end_idx]:
            candidates_page.append(
                {
                    "issue_key_1": c.issue_key_1,
                    "issue_key_2": c.issue_key_2,
                    "similarity": round(c.similarity, 4),
                    "similarity_percent": round(c.similarity * 100, 2),
                    "summary_1": c.summary_1,
                    "summary_2": c.summary_2,
                }
            )

    return render_template(
        "index.html",
        base_url=base_url,
        email=email,
        api_token=api_token,
        iq_token=iq_token,
        project=project,
        issue_type=issue_type,
        summary=_last_summary,
        candidates_page=candidates_page,
        page=page,
        total_pages=total_pages,
        ui_page_size=ui_page_size,
        other_labels_text=other_labels_text,
        exclude_labels_text=exclude_labels_text,
        search_term=search_term,
        threshold_percent=threshold_percent,
        dedup_decisions=_last_dedup_decisions,
        instruction_text=_last_instruction_text,
    )


@web_bp.route("/dedup_results")
def dedup_results():
    """
    Paginated view of AI merge suggestions (deduplicated tests).
    """
    global _last_dedup_decisions
    if not _last_dedup_decisions:
        flash("No AI merge suggestions available. Run Step 3 first.", "error")
        return redirect(url_for("web.index"))

    page_str = request.args.get("page", "1") or "1"
    ui_page_size_str = (
        request.args.get("ui_page_size", str(UI_PAGE_SIZE_DEFAULT))
        or str(UI_PAGE_SIZE_DEFAULT)
    )

    try:
        page = int(page_str)
    except ValueError:
        page = 1
    try:
        ui_page_size = int(ui_page_size_str)
    except ValueError:
        ui_page_size = UI_PAGE_SIZE_DEFAULT
    if ui_page_size not in (20, 50, 100):
        ui_page_size = UI_PAGE_SIZE_DEFAULT

    total_decisions = len(_last_dedup_decisions)
    total_pages = (
        (total_decisions + ui_page_size - 1) // ui_page_size if total_decisions else 1
    )

    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start_idx = (page - 1) * ui_page_size
    end_idx = start_idx + ui_page_size

    page_rows = []
    for global_idx, d in enumerate(
        _last_dedup_decisions[start_idx:end_idx], start=start_idx
    ):
        row = dict(d)
        row["_idx"] = global_idx  # global index into _last_dedup_decisions
        page_rows.append(row)

    dedup_page = page_rows

    return render_template(
        "dedup_results.html",
        dedup_page=dedup_page,
        page=page,
        total_pages=total_pages,
        ui_page_size=ui_page_size,
        total_decisions=total_decisions,
    )


@web_bp.route("/dedup_apply", methods=["POST"])
def dedup_apply():
    """
    Take selected AI merge suggestions and apply them directly in Jira:
      - Create 'Duplicate' issue links.
      - Comment on canonical and duplicate issues.
    """
    global _last_dedup_decisions, _last_summary
    if not _last_dedup_decisions:
        flash("No AI merge suggestions available.", "error")
        return redirect(url_for("web.index"))

    selected_ids = request.form.getlist("selected")
    if not selected_ids:
        flash("No suggestions selected.", "info")
        return redirect(url_for("web.dedup_results"))

    selected_indices = []
    for s in selected_ids:
        try:
            idx = int(s)
        except ValueError:
            continue
        if 0 <= idx < len(_last_dedup_decisions):
            selected_indices.append(idx)

    if not selected_indices:
        flash("No valid suggestions selected.", "info")
        return redirect(url_for("web.dedup_results"))

    if not _last_summary:
        flash("Jira context is missing; please re-run analysis.", "error")
        return redirect(url_for("web.index"))

    base_url = _last_summary.get("base_url", "")
    email = _last_summary.get("email", "")
    api_token = _last_summary.get("api_token", "")
    if not (base_url and email and api_token):
        flash("Jira credentials are missing; please re-run analysis.", "error")
        return redirect(url_for("web.index"))

    jira = JiraClient(base_url, email, api_token)

    applied = 0
    failed = 0

    for idx in selected_indices:
        d = _last_dedup_decisions[idx]
        canonical = d["canonical_key"]
        dup = d["duplicate_key"]
        reason = d.get("reason_to_merge", d.get("reason", ""))

        try:
            # Create a 'Duplicate' link: duplicate -> canonical
            jira.link_issues(canonical, dup, link_type="Duplicate")

            # Add comment on duplicate
            dup_comment = (
                f"This test has been marked as a duplicate of {canonical}.\n"
                f"Reason: {reason}"
            )
            jira.comment_issue(dup, dup_comment)

            # Add comment on canonical
            canon_comment = (
                f"Duplicate test {dup} has been merged into this test.\n"
                f"Reason: {reason}"
            )
            jira.comment_issue(canonical, canon_comment)

            applied += 1
        except Exception as e:
            print("[Jira merge error]", canonical, dup, "->", e)
            failed += 1

    msg = f"Applied {applied} merge(s) to Jira."
    if failed:
        msg += f" {failed} merge(s) failed; see logs for details."
    flash(msg, "info")

    return redirect(url_for("web.dedup_results"))


@web_bp.route("/download/jsonl")
def download_jsonl():
    global _last_tests_jsonl_bytes
    if not _last_tests_jsonl_bytes:
        flash("No normalized test data available. Run Step 2 first.", "error")
        return redirect(url_for("web.index"))
    return send_file(
        io.BytesIO(_last_tests_jsonl_bytes),
        mimetype="application/json",
        as_attachment=True,
        download_name="jira_tests.jsonl",
    )


@web_bp.route("/download/csv")
def download_csv():
    global _last_candidates_csv_bytes
    if not _last_candidates_csv_bytes:
        flash("No similarity data available. Run Step 2 first.", "error")
        return redirect(url_for("web.index"))
    return send_file(
        io.BytesIO(_last_candidates_csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name="duplicate_candidates.csv",
    )


@web_bp.route("/download/dedup_csv")
def download_dedup_csv():
    global _last_dedup_decisions
    if not _last_dedup_decisions:
        flash("No AI merge suggestions available. Run Step 3 first.", "error")
        return redirect(url_for("web.index"))

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["group_id", "canonical_key", "duplicate_key", "reason"])
    for d in _last_dedup_decisions:
        writer.writerow(
            [d["group_id"], d["canonical_key"], d["duplicate_key"], d["reason"]]
        )

    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name="ai_merge_suggestions.csv",
    )


@web_bp.route("/ai_fact", methods=["GET"])
def ai_fact():
    iq_base_url = current_app.config.get("DEFAULT_IQ_BASE_URL", "https://api.iq.cudasvc.com")
    iq_token = ""
    if _last_summary:
        iq_token = _last_summary.get("iq_token", "")

    fact = fetch_ai_fact(iq_base_url, iq_token)
    return jsonify({"fact": fact})