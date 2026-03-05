from dataclasses import dataclass
from typing import Any, List


@dataclass
class TestCase:
    key: str
    summary: Any
    description: Any
    created: str
    labels: List[str]
    components: List[str]


@dataclass
class CandidatePair:
    issue_key_1: str
    issue_key_2: str
    similarity: float
    summary_1: str
    summary_2: str

    @property
    def similarity_percent(self) -> str:
        """Return similarity as a percentage string."""
        return f"{self.similarity * 100:.2f}"