import requests


class JiraClient:
    """
    Simple Jira client that uses /rest/api/3/search/jql with nextPageToken.
    """

    def __init__(self, base_url: str, email: str, api_token: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (email, api_token)

    def fetch_issues(self, jql: str, page_size: int = 100):
        url = f"{self.base_url}/rest/api/3/search/jql"

        all_issues = []
        next_token = None
        page = 0

        while True:
            params = {
                "jql": jql,
                "fields": "summary,description,created,labels,components",
                "maxResults": str(page_size),
            }
            if next_token:
                params["nextPageToken"] = next_token

            resp = requests.get(url, params=params, auth=self.auth)
            if not resp.ok:
                try:
                    err = resp.json()
                except Exception:
                    err = resp.text
                raise RuntimeError(
                    f"Jira API error (status {resp.status_code}): {err}"
                )

            data = resp.json()
            issues = data.get("issues", []) or []
            all_issues.extend(issues)

            is_last = data.get("isLast", True)
            next_token = data.get("nextPageToken")

            if is_last or not issues or not next_token:
                break

            page += 1

        return all_issues

    def link_issues(self, inward_issue: str, outward_issue: str, link_type: str = "Duplicate"):
        """
        Create a link between two issues.

        Args:
            inward_issue: The inward issue key (canonical)
            outward_issue: The outward issue key (duplicate)
            link_type: The type of link (default: "Duplicate")
        """
        url = f"{self.base_url}/rest/api/3/issueLink"

        payload = {
            "type": {"name": link_type},
            "inwardIssue": {"key": inward_issue},
            "outwardIssue": {"key": outward_issue}
        }

        resp = requests.post(url, json=payload, auth=self.auth)
        if not resp.ok:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            raise RuntimeError(
                f"Jira API error creating link (status {resp.status_code}): {err}"
            )
        return resp

    def comment_issue(self, issue_key: str, comment: str):
        """
        Add a comment to a Jira issue.

        Args:
            issue_key: The issue key to comment on
            comment: The comment text
        """
        url = f"{self.base_url}/rest/api/3/issue/{issue_key}/comment"

        payload = {
            "body": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [
                            {
                                "type": "text",
                                "text": comment
                            }
                        ]
                    }
                ]
            }
        }

        resp = requests.post(url, json=payload, auth=self.auth)
        if not resp.ok:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            raise RuntimeError(
                f"Jira API error adding comment (status {resp.status_code}): {err}"
            )
        return resp

    def update_issue_summary(self, issue_key: str, new_summary: str):
        """
        Update the summary field of a Jira issue.

        Args:
            issue_key: The issue key to update
            new_summary: The new summary text
        """
        url = f"{self.base_url}/rest/api/3/issue/{issue_key}"

        payload = {
            "fields": {
                "summary": new_summary
            }
        }

        resp = requests.put(url, json=payload, auth=self.auth)
        if not resp.ok:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            raise RuntimeError(
                f"Jira API error updating summary (status {resp.status_code}): {err}"
            )
        return resp

    def transition_issue(self, issue_key: str, status_name: str):
        """
        Transition a Jira issue to a new status.

        Args:
            issue_key: The issue key to transition
            status_name: The target status name (e.g., "Rejected")
        """
        # First, get available transitions for the issue
        transitions_url = f"{self.base_url}/rest/api/3/issue/{issue_key}/transitions"
        resp = requests.get(transitions_url, auth=self.auth)

        if not resp.ok:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            raise RuntimeError(
                f"Jira API error fetching transitions (status {resp.status_code}): {err}"
            )

        transitions_data = resp.json()
        transitions = transitions_data.get("transitions", [])

        # Find the transition ID for the target status
        transition_id = None
        for transition in transitions:
            to_status = transition.get("to", {})
            if to_status.get("name", "").lower() == status_name.lower():
                transition_id = transition.get("id")
                break

        if not transition_id:
            # If status not found in available transitions, just skip silently
            # This allows the merge to continue even if status transition isn't available
            return None

        # Perform the transition
        payload = {
            "transition": {
                "id": transition_id
            }
        }

        resp = requests.post(transitions_url, json=payload, auth=self.auth)
        if not resp.ok:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            raise RuntimeError(
                f"Jira API error transitioning issue (status {resp.status_code}): {err}"
            )
        return resp