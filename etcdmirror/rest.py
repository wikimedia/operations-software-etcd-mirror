"""
  Rest interface for etcd2mirror
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  A simple http server that exposes some data on the replication process
"""
from prometheus_client.twisted import MetricsResource
from twisted.web.resource import Resource


class NotFound(Resource):
    """
    Handle unknown entries
    """

    isLeaf = True

    def render_GET(self, request):
        request.setResponseCode(404)
        return "The desired url {} was not found".format(request.uri.decode()).encode()


class ServerRoot(Resource):
    """Root url resource"""

    def getChild(self, path, request):
        path_str = path.decode()
        if path_str == "lag":
            return LagCalculator()
        elif path_str == "metrics":
            return MetricsResource()
        else:
            return NotFound()

    def render_GET(self, request):
        return b"""
/lag: Replication lag (in term of etcd indexes)
/metrics: metrics in a format useful for prometheus
"""


class LagCalculator(Resource):
    replica_idx = 0
    origin_idx = 0

    @classmethod
    def setReplica(cls, idx):
        cls.replica_idx = int(idx)

    @classmethod
    def setOrigin(cls, idx):
        cls.origin_idx = int(idx)

    @classmethod
    def getLag(cls):
        return cls.origin_idx - cls.replica_idx

    def render_GET(self, request):
        return b"%d\n" % self.getLag()
