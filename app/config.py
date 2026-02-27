import os


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-key")

    # Jira base URL
    DEFAULT_JIRA_BASE_URL = os.environ.get(
        "DEFAULT_JIRA_BASE_URL",
        "https://cuda.atlassian.net",
    )

    # IQ base URL
    DEFAULT_IQ_BASE_URL = os.environ.get(
        "DEFAULT_IQ_BASE_URL",
        "https://api.iq.cudasvc.com",
    )