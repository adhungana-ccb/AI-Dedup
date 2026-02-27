import time
import requests
from typing import List, Dict, Any


class JiraClient:
    """
    Jira client for:
      - fetching issues via JQL (using /rest/api/3/search/jql)
      - linking issues (e.g., marking duplicates)
      - adding comments
    """

    def __init__(self, base_url: str, email: str, api_token: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (email, api_token)
        self.headers = {
            "Accept": "application/json",
            # No Content-Type needed for GET requests
        }

    def fetch_issues(
        self,
        jql: str,
        page_size: int = 200,
        max_issues: int = 5000,
        max_rate_limit_retries: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Fetch issues matching the given JQL using Jira Cloud's new search endpoint:

          GET /rest/api/3/search/jql

        This implementation:
          - Sends JQL and paging as query parameters.
          - Requests only the fields we use.
          - Paginates until:
              * we hit max_issues, OR
              * the server returns fewer than page_size results, OR
              * no new issue keys are seen.
          - Deduplicates issues by key to avoid self-comparisons later.
          - Handles 429 rate-limit responses with backoff and a maximum retry count.
        """
        seen_keys = set()
        issues: List[Dict[str, Any]] = []
        start_at = 0
        rate_limit_retries = 0

        while len(issues) < max_issues:
            remaining = max_issues - len(issues)
            page_size_effective = min(page_size, remaining)

            params = {
                "jql": jql,
                "maxResults": page_size_effective,
                "startAt": start_at,
                "fields": "summary,description,labels,components,created",
            }
            url = f"{self.base_url}/rest/api/3/search/jql"
            resp = requests.get(
                url, headers=self.headers, auth=self.auth, params=params, timeout=30
            )

            # Handle rate limiting: 429
            if resp.status_code == 429:
                rate_limit_retries += 1
                if rate_limit_retries > max_rate_limit_retries:
                    print(
                        "=== Jira API RATE LIMIT (429) ===\n"
                        f"URL: {resp.url}\n"
                        "Maximum rate-limit retries exceeded. Stopping fetch early.\n"
                        "=== END Jira API RATE LIMIT (429) ==="
                    )
                    break

                retry_after = resp.headers.get("Retry-After")
                try:
                    sleep_seconds = int(retry_after) if retry_after is not None else 10
                except ValueError:
                    sleep_seconds = 10
                print(
                    "=== Jira API RATE LIMIT (429) ===\n"
                    f"URL: {resp.url}\n"
                    f"Retrying after {sleep_seconds} seconds "
                    f"(attempt {rate_limit_retries}/{max_rate_limit_retries})...\n"
                    "=== END Jira API RATE LIMIT (429) ==="
                )
                time.sleep(sleep_seconds)
                continue  # Retry the same page

            # Reset rate-limit retry counter on successful response
            rate_limit_retries = 0

            if not resp.ok:
                try:
                    err_json = resp.json()
                except Exception:
                    err_json = resp.text
                print("=== Jira API ERROR (HTTP) ===")
                print(f"Status: {resp.status_code}")
                print(f"URL: {resp.url}")
                print("Request params:", params)
                print("Response body:", err_json)
                print("=== END Jira API ERROR (HTTP) ===")
                raise RuntimeError(
                    f"Error fetching from Jira (status {resp.status_code}): {err_json}"
                )

            data = resp.json()
            batch = data.get("issues", [])

            # Debug: first page info
            if start_at == 0:
                sample_keys = [i.get("key") for i in batch[:3]]
                print(
                    f"[Jira debug] search/jql page_size={page_size_effective}, "
                    f"first_page_count={len(batch)}, first_keys={sample_keys}"
                )

            # Deduplicate by issue key
            new_keys = 0
            for issue in batch:
                key = issue.get("key")
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    issues.append(issue)
                    new_keys += 1

            fetched = len(batch)
            start_at += fetched

            # Stop if:
            # - fewer than page_size_effective issues returned (end of result set), OR
            # - no new keys added (startAt not effective / repeating), OR
            # - we have reached max_issues.
            if fetched < page_size_effective or new_keys == 0 or len(issues) >= max_issues:
                break

        print(f"[Jira debug] Total unique issues fetched via search/jql: {len(issues)}")
        if len(issues) >= max_issues:
            print(
                "[Jira debug] Hit max_issues cap; further matching issues were not fetched "
                "to avoid excessive API calls."
            )

        return issues

    def link_issues(self, canonical_key: str, duplicate_key: str, link_type: str = "Duplicate"):
        """
        Create an issue link of the given type between duplicate and canonical issues.
        """
        url = f"{self.base_url}/rest/api/3/issueLink"
        payload = {
            "type": {"name": link_type},
            "inwardIssue": {"key": canonical_key},
            "outwardIssue": {"key": duplicate_key},
        }
        resp = requests.post(
            url,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            auth=self.auth,
            json=payload,
            timeout=30,
        )
        if not resp.ok:
            print(
                "[Jira link error]",
                canonical_key,
                duplicate_key,
                resp.status_code,
                resp.text,
            )
        resp.raise_for_status()

    def comment_issue(self, issue_key: str, body: str):
        """
        Add a comment to an issue.
        """
        url = f"{self.base_url}/rest/api/3/issue/{issue_key}/comment"
        payload = {"body": body}
        resp = requests.post(
            url,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            auth=self.auth,
            json=payload,
            timeout=30,
        )
        if not resp.ok:
            print("[Jira comment error]", issue_key, resp.status_code, resp.text)
        resp.raise_for_status()