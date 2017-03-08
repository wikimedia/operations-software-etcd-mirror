from argparse import ArgumentParser
import logging
import signal
import sys

import etcd
from prometheus_client import Counter, Summary, Histogram
from urlparse import urlparse
from twisted.internet import reactor, threads, defer
from twisted.web.server import Site

from etcdmirror.reader import Etcd2Reader
from etcdmirror.log import log, LogObserver
from etcdmirror.writer import Etcd2Writer
from etcdmirror.rest import ServerRoot, LagCalculator


def read_config(url):
    parsed = urlparse(url)
    if '@' in parsed.netloc:
        auth, netloc = parsed.netloc.split('@')
        user, passwd = auth.split(':')
    else:
        netloc = parsed.netloc
        user = passwd = None
    (h, p) = netloc.split(':')

    return {'host': h, 'port': int(p), 'username': user, 'password': passwd,
            'protocol': parsed.scheme, 'allow_reconnect': False}


def cli_args():
    parser = ArgumentParser(
        description="Etcd MirrorMaker.",
        epilog="Allows replicating a specific prefix in one etcd "
        "cluster to a different prefix on another cluster."
    )
    parser.add_argument('-d', '--debug', action='store_true',
                        help="Show debug output. Definitely *very* verbose")
    parser.add_argument('--reload', action='store_true',
                        help="Wipe out the data on the receiving cluster, and load everything from the source")
    parser.add_argument(
        '--src-prefix', default='/',
        help="Prefix to replicate from the source. Defaults to '/'")
    parser.add_argument('--strip', action='store_true',
                        help="Strip the source prefix from the keys.")
    parser.add_argument(
        '--dst-prefix', default='/replica',
        help="Prefix to replicate to on the destination. Defaults to '/replica'"
    )
    parser.add_argument('--port', help="port to run the web interface on",
                        type=int, default=8000)
    parser.add_argument('src', metavar="SRC", help="Full url of the source etcd machine.")
    parser.add_argument('dst', metavar="DST", help="Full url of the destination etcd machine.")
    return parser.parse_args()


def main():
    if '--version' in sys.argv:
        print "0.0.3"
        sys.exit(0)

    args = cli_args()
    if args.debug:
        LogObserver.level = logging.DEBUG
    src_conf = read_config(args.src)
    dst_conf = read_config(args.dst)
    source = etcd.Client(**src_conf)
    destination = etcd.Client(**dst_conf)
    read = Etcd2Reader(source, args.src_prefix)
    if args.strip:
        write = Etcd2Writer(destination, args.dst_prefix, args.src_prefix)
    else:
        write = Etcd2Writer(destination, args.dst_prefix)
    controller = ReplicationController(read, write)
    factory = Site(ServerRoot())
    reactor.listenTCP(args.port, factory)
    if args.reload:
        controller.reload_data()
    try:
        idx = controller.current_index
    except etcd.EtcdKeyNotFound:
        log.error("The replication key could not be found. Restart with --reload!")
        sys.exit(1)
    log.info('Starting replication at %s', idx)
    # This will start a chain of deferred calls
    controller.replicate(idx)
    reactor.run()
    if controller.has_failures:
        sys.exit(1)

load_time = Summary('load_time', 'Dump and load time (seconds)')


class ReplicationController(object):

    def __init__(self, read, write):
        self.has_failures = False
        self.running = True
        self.reader = read
        self.writer = write
        self.replicated_events = Counter('etcd_replicated_events', 'Replica events treated')
        self.write_latency = Histogram(
            'etcd_write_latency',
            'Etcd write latencies', ['url', 'prefix']
        ).labels(
            url=self.writer.client.base_uri,
            prefix=self.writer.prefix,
        )
        for sig in [signal.SIGTERM, signal.SIGHUP, signal.SIGINT]:
            signal.signal(sig, self._sighandler)

    def _sighandler(self, signum, frame):
        self.running = False
        reactor.callLater(0, reactor.stop)
        sys.exit(0)

    @property
    def current_index(self):
        log.info("Current index read from %s", self.writer.idx)
        return int(self.writer.client.read(self.writer.idx).value)

    @load_time.time()
    def reload_data(self):
        log.info("Re-loading the etcd data from the source cluster")
        log.info("Removing old data from the destination cluster")

        if self.writer.cleanup():
            log.info("Now copying over the initial data")
            root = self.reader.all_objects()
            self.writer.load_from_dump(root)
        else:
            log.critical("Stopping execution")
            sys.exit(1)

    def replicate(self, idx):
        """
        Replication loop
        """
        d = threads.deferToThread(self.read_write, idx+1)
        d.addCallback(self.replicate).addErrback(self.manage_failure)
        d.addCallback(lambda _: self.replicated_events.inc(1.0))

    def replicate_blocking(self, idx):
        log.info("Replicating to %s, starting from index %s", self.writer.prefix, idx+1)
        while True:
            idx = self.read_write(idx+1)

    def manage_failure(self, failure):
        if failure.check(defer.CancelledError):
            return None
        elif failure.check(etcd.EtcdEventIndexCleared):
            log.error("The current replication index is not available anymore in the etcd source cluster.")
            log.error("Restart the process with --reload instead.")
            reactor.stop()
            self.has_failures = True
        elif failure.check(SystemExit):
            return None

    def read_write(self, idx):
        while self.running:
            try:
                obj = self.reader.read(idx)
                break
            except etcd.EtcdWatchTimedOut:
                pass
        LagCalculator.setOrigin(obj.etcd_index)
        log.info('Replicating key %s at index %s', obj.key, idx)
        with self.write_latency.time():
            idx1 = self.writer.write(obj)
        LagCalculator.setReplica(idx1)
        if not idx1:
            # Replication encountered a fatal error
            log.err("Stopping the process. Last valid index: %d", idx)
            reactor.stop()
            self.running = False
            self.has_failures = True
        return idx1


if __name__ == '__main__':
    main()
