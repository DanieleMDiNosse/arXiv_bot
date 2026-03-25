from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

import arXiv_bot


class EntriesToRecentPapersTests(unittest.TestCase):
    def _entry(self, *, published: datetime, updated: datetime, idx: int) -> SimpleNamespace:
        return SimpleNamespace(
            published=published.isoformat(),
            updated=updated.isoformat(),
            authors=[SimpleNamespace(name="Author")],
            id=f"http://arxiv.org/abs/1234.{idx:04d}",
            title=f"Title {idx}",
            summary="Summary",
            links=[],
            tags=[{"term": "cs.AI"}],
        )

    def test_keeps_recently_updated_entries_even_if_published_is_old(self) -> None:
        now = datetime.now(timezone.utc)
        entries = [
            self._entry(
                published=now - timedelta(days=3650),
                updated=now - timedelta(hours=2),
                idx=1,
            ),
            self._entry(
                published=now - timedelta(days=2000),
                updated=now - timedelta(hours=30),
                idx=2,
            ),
            self._entry(
                published=now - timedelta(days=1000),
                updated=now - timedelta(hours=1),
                idx=3,
            ),
        ]

        papers = arXiv_bot.entries_to_recent_papers(entries, hours_back=24)

        self.assertEqual([paper.arxiv_id for paper in papers], ["1234.0001", "1234.0003"])


if __name__ == "__main__":
    unittest.main()
