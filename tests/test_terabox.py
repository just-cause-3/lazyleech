import unittest
from unittest.mock import AsyncMock, Mock

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


class TeraboxBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_allows_https_redirect_within_same_terabox_site(self):
        from terabox_resolver import TeraboxResolver

        class Response:
            def __init__(self, status, location="", body=""):
                self.status = status
                self.headers = {"Location": location} if location else {}
                self.body = body

            async def text(self):
                return self.body

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        fake_session = Mock()
        fake_session.get.side_effect = [
            Response(302, "https://www.1024terabox.com/ai/index"),
            Response(200, body="<html></html>"),
        ]
        resolver = TeraboxResolver(
            fake_session,
            "disposable-cookie",
            "https://dm.1024terabox.com/ai/index",
        )

        await resolver._bootstrap()

        self.assertEqual("https://www.1024terabox.com", resolver.origin)
        self.assertEqual(2, fake_session.get.call_count)
        self.assertIn(
            "ndus=disposable-cookie",
            fake_session.get.call_args.kwargs["headers"]["Cookie"],
        )

    async def test_rejects_bootstrap_redirect_to_unrelated_site(self):
        from terabox_resolver import TeraboxResolver

        class RedirectResponse:
            status = 302
            headers = {"Location": "https://attacker.example/ai/index"}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        fake_session = Mock()
        fake_session.get.return_value = RedirectResponse()
        resolver = TeraboxResolver(
            fake_session,
            "disposable-cookie",
            "https://dm.1024terabox.com/ai/index",
        )

        with self.assertRaisesRegex(TeraboxError, "outside its approved site"):
            await resolver._bootstrap()


class TeraboxVerificationFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_need_verify_retries_public_share_without_cookie(self):
        from terabox_resolver import TeraboxFile, TeraboxResolver

        resolver = TeraboxResolver(
            Mock(),
            "disposable-cookie",
            "https://dm.1024terabox.com/ai/index",
        )
        resolver._json_get = AsyncMock(
            side_effect=[
                {"errno": 4000020, "errmsg": "need verify"},
                {"errno": 0},
            ]
        )

        async def add_file(_surl, _remote_dir, _relative_dir, output):
            output.append(
                TeraboxFile(
                    name="file.zip",
                    relative_path="file.zip",
                    size=123,
                    download_url="https://dm-d.1024terabox.com/file",
                )
            )

        resolver._walk = AsyncMock(side_effect=add_file)

        files = await resolver.resolve(
            "https://1024terabox.com/s/1qSH41vcEGsmk7vSnunpt_w"
        )

        self.assertEqual(["file.zip"], [item.name for item in files])
        self.assertFalse(resolver._share_authenticated)
        first_call, second_call = resolver._json_get.await_args_list
        self.assertNotIn("authenticated", first_call.kwargs)
        self.assertFalse(second_call.kwargs["authenticated"])

    async def test_need_verify_v2_has_actionable_error(self):
        from terabox_resolver import TeraboxResolver

        resolver = TeraboxResolver(
            Mock(),
            "disposable-cookie",
            "https://dm.1024terabox.com/ai/index",
        )
        resolver._json_get = AsyncMock(
            side_effect=[
                {"errno": 400210, "errmsg": "need verify_v2"},
                {"errno": 400210, "errmsg": "need verify_v2"},
            ]
        )

        with self.assertRaisesRegex(
            TeraboxError,
            "requires browser/account verification",
        ):
            await resolver.resolve(
                "https://1024terabox.com/s/1qSH41vcEGsmk7vSnunpt_w"
            )

    async def test_anonymous_requests_do_not_send_cookie_or_browserid(self):
        from terabox_resolver import TeraboxResolver

        class JsonResponse:
            status = 200

            async def json(self, content_type=None):
                return {"errno": 0}

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

        fake_session = Mock()
        fake_session.get.return_value = JsonResponse()
        resolver = TeraboxResolver(
            fake_session,
            "disposable-cookie",
            "https://dm.1024terabox.com/ai/index",
        )

        await resolver._json_get(
            "/api/shorturlinfo",
            {"shorturl": "1share", "root": "1"},
            authenticated=False,
        )

        headers = fake_session.get.call_args.kwargs["headers"]
        self.assertNotIn("Cookie", headers)
        self.assertNotIn("browserid", str(headers).lower())


if __name__ == "__main__":
    unittest.main()
