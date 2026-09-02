import unittest
from unittest.mock import Mock

from terabox_resolver import (
    TeraboxError,
    _cookie_site,
    _download_url,
    extract_surl,
    normalize_endpoint,
)


class TeraboxUrlTests(unittest.TestCase):
    def test_extracts_sharing_link(self):
        self.assertEqual(
            "AljYIGbfMoFYQxf4aq-4WA",
            extract_surl(
                "https://www.terabox.com/sharing/link?surl=AljYIGbfMoFYQxf4aq-4WA"
            ),
        )

    def test_extracts_short_path_without_prefix(self):
        self.assertEqual("abc_DEF-123", extract_surl("https://terabox.com/s/1abc_DEF-123"))

    def test_rejects_unrecognized_input(self):
        with self.assertRaises(TeraboxError):
            extract_surl("https://example.com/no-share-code-here/")

    def test_normalizes_configured_ai_endpoint(self):
        self.assertEqual(
            ("https://dm.1024terabox.com", "/ai/index"),
            normalize_endpoint("https://dm.1024terabox.com/ai/index"),
        )

    def test_rejects_insecure_endpoint(self):
        with self.assertRaises(TeraboxError):
            normalize_endpoint("http://dm.1024terabox.com/ai/index")

    def test_cleans_transient_parameters_and_adds_origin(self):
        url = _download_url(
            "https://cdn.example/file.zip?token=ok&chkv=0&dp-logid=12&sh=1"
        )
        self.assertIn("token=ok", url)
        self.assertIn("origin=dlna", url)
        self.assertNotIn("chkv", url)
        self.assertNotIn("dp-logid", url)

    def test_cookie_site_allows_regional_sibling(self):
        self.assertEqual(
            _cookie_site("dm.1024terabox.com"),
            _cookie_site("dm-d.1024terabox.com"),
        )

    def test_cookie_site_rejects_storage_domain(self):
        self.assertNotEqual(
            _cookie_site("dm.1024terabox.com"),
            _cookie_site("d13-dm.freeterabox.com"),
        )


class _FakeContent:
    async def read(self, _size):
        return b'{"error_code":302}'


class _FakeResponse:
    status = 302
    headers = {"Location": "https://d13-dm.freeterabox.com/signed/file.rar"}
    url = "https://dm-d.1024terabox.com/file/signed"
    content = _FakeContent()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class TeraboxAuthorizationTests(unittest.IsolatedAsyncioTestCase):
    async def test_exchanges_authenticated_hop_for_cookie_free_url(self):
        from terabox_resolver import TeraboxResolver

        fake_session = Mock()
        fake_session.get.return_value = _FakeResponse()
        resolver = TeraboxResolver(
            fake_session,
            "disposable-cookie",
            "https://dm.1024terabox.com/ai/index",
        )

        result = await resolver.authorize_download_url(
            "https://dm-d.1024terabox.com/file/signed"
        )

        self.assertEqual(
            "https://d13-dm.freeterabox.com/signed/file.rar", result
        )
        request_headers = fake_session.get.call_args.kwargs["headers"]
        self.assertIn("ndus=disposable-cookie", request_headers["Cookie"])
        self.assertFalse(fake_session.get.call_args.kwargs["allow_redirects"])

    async def test_refuses_cookie_on_unrelated_download_host(self):
        from terabox_resolver import TeraboxResolver

        fake_session = Mock()
        resolver = TeraboxResolver(
            fake_session,
            "disposable-cookie",
            "https://dm.1024terabox.com/ai/index",
        )
        with self.assertRaises(TeraboxError):
            await resolver.authorize_download_url(
                "https://attacker.example/file/signed"
            )
        fake_session.get.assert_not_called()


if __name__ == "__main__":
    unittest.main()
