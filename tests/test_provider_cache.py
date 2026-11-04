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

    def test_both_provider_routes_use_fresh_reads_and_unrelated_urls_stay_unchanged(self):
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
        energex = parse_qs(urlsplit(requests[1].full_url).query)
        self.assertEqual(energex["SQ_SESSION_MODE"], ["read_only_async"])
        self.assertIn("_", energex)
        self.assertEqual(requests[2].full_url, urls[2])

    def test_energex_summary_council_and_search_each_get_fresh_responses(self):
        requests = []
        def open_url(request, **_options):
            requests.append(request)
            response = MagicMock()
            # Reproduce a CDN serving stale data for the old, fixed URL.
            current = "_" in parse_qs(urlsplit(request.full_url).query)
            response.__enter__.return_value.read.return_value = b"current" if current else b"stale"
            return response
        urls = [ENERGEX_API + "summary", ENERGEX_API + "council?council=",
                ENERGEX_API + "search?council=Scenic+Rim+Regional", ENERGEX_API + "summary"]
        with patch("write_provider_council_counts.urlopen", side_effect=open_url):
            self.assertEqual([fetch_text(url) for url in urls], ["current"] * 4)
        self.assertEqual(len({request.full_url for request in requests}), 4)
        self.assertIn("council=", requests[1].full_url)
        self.assertEqual(parse_qs(urlsplit(requests[2].full_url).query)["council"], ["Scenic Rim Regional"])


if __name__ == "__main__":
    unittest.main()
