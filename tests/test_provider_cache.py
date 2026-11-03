import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

from write_provider_council_counts import ENERGEX_API, ERGON_TEXT, fetch_text


class ProviderCacheTests(unittest.TestCase):
    def responses(self):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = b"current public outage page"
        return response

    def test_ergon_poll_requests_get_distinct_cache_keys_and_keep_read_only_parameters(self):
        requests = []
        def open_url(request, **_options):
            requests.append(request)
            return self.responses()
        with patch("write_provider_council_counts.urlopen", side_effect=open_url), \
                patch("write_provider_council_counts.time_ns", side_effect=[100, 101]):
            for _ in range(2):
                self.assertEqual(fetch_text(ERGON_TEXT + "&extra=keep&_=old"), "current public outage page")
        self.assertNotEqual(requests[0].full_url, requests[1].full_url)
        for i, request in enumerate(requests):
            query = parse_qs(urlsplit(request.full_url).query)
            self.assertEqual(query["_"], [str(100 + i)])
            self.assertEqual(query["SQ_SESSION_MODE"], ["read_only_async"])
            self.assertEqual(query["extra"], ["keep"])

    def test_public_ergon_page_uses_read_only_mode_and_other_sources_keep_their_urls(self):
        urls = [ERGON_TEXT.split("?")[0], ENERGEX_API + "summary?SQ_SESSION_MODE=read_only_async",
                "https://www.ergon.com.au/unrelated"]
        requests = []
        def open_url(request, **_options):
            requests.append(request)
            return self.responses()
        with patch("write_provider_council_counts.urlopen", side_effect=open_url):
            for url in urls:
                fetch_text(url)
        query = parse_qs(urlsplit(requests[0].full_url).query)
        self.assertEqual(query["SQ_SESSION_MODE"], ["read_only_async"])
        self.assertIn("_", query)
        self.assertEqual([r.full_url for r in requests[1:]], urls[1:])


if __name__ == "__main__":
    unittest.main()
