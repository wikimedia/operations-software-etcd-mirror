import twisted.trial.unittest
from twisted.web.test.test_web import DummyRequest
from twisted.web.wsgi import WSGIResource

from etcdmirror.rest import LagCalculator, NotFound, ServerRoot


class ServerRootTests(twisted.trial.unittest.TestCase):
    server = ServerRoot()

    def _test_render(self, request, resource, responseCode=None, response=None):
        r = resource.render(request)
        self.assertIsInstance(r, bytes)
        if responseCode:
            self.assertEqual(request.responseCode, responseCode)
        if response:
            self.assertEqual(r, response)

    def test_notfound(self):
        request = DummyRequest(b"")
        request.uri = b"http://dummy/foo"
        child = self.server.getChild(b"foo", request)
        self.assertIsInstance(child, NotFound)
        self._test_render(request, child, 404, b"The desired url http://dummy/foo was not found")

    def test_get(self):
        request = DummyRequest(b"")

        self._test_render(
            request,
            self.server,
            response=b"""
/lag: Replication lag (in term of etcd indexes)
/metrics: metrics in a format useful for prometheus
""",
        )

    def test_lag(self):
        request = DummyRequest(b"")
        request.uri = b"http://dummy/lag"
        child = self.server.getChild(b"lag", request)
        self.assertIsInstance(child, LagCalculator)
        self._test_render(request, child)

    def test_metrics(self):
        child = self.server.getChild(b"metrics", DummyRequest(b""))
        self.assertIsInstance(child, WSGIResource)
