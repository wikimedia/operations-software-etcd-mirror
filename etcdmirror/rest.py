"""
  Rest interface for etcd2mirror
  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  A simple http server that exposes some data on the replication process
"""
from twisted.web.resource import Resource
from prometheus_client.twisted import MetricsResource


class NotFound(Resource):
    """
    Handle unknown entries
    """
    isLeaf = True

    def render_GET(self, request):
        request.setResponseCode(404)
        return "The desired url {} was not found".format(request.uri)


class ServerRoot(Resource):
    """Root url resource"""

    def getChild(self, path, request):
        if path == 'lag':
            return LagCalculator()
        elif path == 'metrics':
            return MetricsResource()
        else:
            return NotFound()

    def render_GET(self, request):
        return """
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

    def render_GET(self, request):
        lag = self.origin_idx - self.replica_idx
        return "%d\n" % lag
