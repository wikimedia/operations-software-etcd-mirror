import etcd
import mock
import twisted.internet
import twisted.test.proto_helpers
import twisted.trial.unittest
from twisted.python import failure

from etcdmirror import main, reader, writer


class TestReplicationController(twisted.trial.unittest.TestCase):
    def setUp(self):
        self.reader = mock.MagicMock(autospec=reader.Etcd2Reader)
        self.writer = mock.MagicMock(autospec=writer.Etcd2Writer)
        with mock.patch("etcdmirror.main.Counter"):
            with mock.patch("etcdmirror.main.Histogram"):
                self.rc = main.ReplicationController(self.reader, self.writer)
        self.reactor = twisted.test.proto_helpers.MemoryReactor()

    def test_init(self):
        """
        Test object initialization
        """
        self.assertFalse(self.rc.has_failures)
        self.assertTrue(self.rc.running)
        self.assertEqual(self.reader, self.rc.reader)
        self.assertEqual(self.writer, self.rc.writer)

    def test_sighandler(self):
        """
        test the signal handler
        """
        with mock.patch("etcdmirror.main.reactor") as mock_reactor:
            self.rc._sighandler(1, 10)
            self.assertFalse(self.rc.running)
            mock_reactor.callLater.assert_called_with(0, mock_reactor.stop)

    def test_fail(self):
        with mock.patch("etcdmirror.main.reactor") as mock_reactor:
            mock_reactor.running = True
            self.rc._fail("reason")
            self.assertFalse(self.rc.running)
            self.assertTrue(self.rc.has_failures)
            mock_reactor.stop.assert_called_with()
            mock_reactor.running = False
            mock_reactor.stop.reset_mock()
            with mock.patch("etcdmirror.log.log.critical") as mock_log:
                self.rc._fail("reason")
                mock_log.assert_called_with("reason")

    def test_current_index(self):
        """
        Test the value of the ReplicationController.current_index property
        """
        obj = mock.Mock()
        obj.value = "123456"
        self.writer.client.read.return_value = obj
        self.writer.idx = "/__replication/test"
        self.assertEqual(self.rc.current_index, 123456)
        # now let's test the case in which the key is not found. The exception
        # should not be intercepted as it's tested for.
        self.writer.client.read.side_effect = etcd.EtcdKeyNotFound("no")
        with self.assertRaises(etcd.EtcdKeyNotFound):
            self.rc.current_index

    def test_reload_data(self):
        """
        Test calls made when reloading data
        """
        # Standard reload, nothing out of the ordinary
        self.writer.cleanup.return_value = True
        obj = mock.Mock(return_value=[])
        self.reader.all_objects.return_value = obj
        self.rc.reload_data()
        self.writer.load_from_dump.assert_called_with(obj)
        # Cleanup fails
        self.writer.cleanup.return_value = False
        self.writer.load_from_dump = mock.Mock()
        self.rc.reload_data()
        self.assertTrue(self.rc.has_failures)
        self.writer.load_from_dump.assert_not_called()
        # Load from dump fails, exceptions are handled, and a failure is
        # reported
        self.rc.has_failures = False
        self.writer.cleanup.return_value = True
        self.writer.load_from_dump.side_effect = etcd.EtcdAlreadyExist("test")
        self.rc.reload_data()
        self.assertTrue(self.rc.has_failures)

    def test_manage_failure(self):
        """
        Test behaviour of ReplicationController.manage_failure
        """
        # Cancel deferreds are ignored silently
        failure_deferred = failure.Failure("", twisted.internet.defer.CancelledError)
        failure_ev_cleared = failure.Failure("nope", etcd.EtcdEventIndexCleared)
        failure_system_exit = failure.Failure("1", SystemExit)
        failure_generic = failure.Failure("test", ValueError)
        self.rc._fail = mock.Mock()
        self.assertEqual(self.rc.manage_failure(failure_deferred), None)
        self.rc._fail.assert_not_called()
        self.assertEqual(self.rc.manage_failure(failure_ev_cleared), None)
        self.rc._fail.assert_called_with(
            "The current replication index is not available anymore in the etcd source cluster."
        )
        # What about other failures?
        self.rc.manage_failure(failure_generic)
        self.rc._fail.assert_called_with("Generic error: test")
        self.rc._fail.reset_mock()
        self.rc.manage_failure(failure_system_exit)
        self.rc._fail.assert_not_called()

    def test_read_write_not_running(self):
        """
        Test behaviour of ReplicationController.read_write
        """
        # When not running, we break out of the loop
        self.rc.running = False
        self.rc.read_write(1)
        self.reader.read.assert_not_called()

    def test_read_write_successful(self):
        """
        Test a successful ReplicationController.read_write run
        """
        # Mock a successful read
        obj = mock.Mock()
        obj.etcd_index = 130
        obj.modifiedIndex = 103
        obj.key = "/test/test1"
        self.reader.read.return_value = obj
        self.writer.write.return_value = obj.modifiedIndex
        with mock.patch("etcdmirror.main.LagCalculator") as mock_rest:
            self.rc.running = True
            self.rc.has_failures = False
            self.assertEqual(self.rc.read_write(102), obj.modifiedIndex)
            self.reader.read.assert_called_with(102)
            self.writer.write.assert_called_with(obj)
            mock_rest.setOrigin.assert_called_with(obj.etcd_index)
            mock_rest.setReplica.assert_called_with(obj.modifiedIndex)
            self.assertFalse(self.rc.has_failures)

    def test_read_write_error(self):
        """
        Test behaviour of ReplicationController.read_write when writing fails
        """
        # Now let's test what's the behaviour when writing fails
        self.writer.write.return_value = False
        with mock.patch("etcdmirror.main.reactor") as mock_reactor:
            self.rc.has_failures = False
            self.rc.read_write(102)
            mock_reactor.stop.assert_called_with()
            self.assertTrue(self.rc.has_failures)

    def test_read_write_timeout(self):
        """
        Test behaviour of ReplicationController.read_write when read times out
        """
        # Simulate a timeout
        obj = mock.Mock()
        obj.etcd_index = 130
        obj.modifiedIndex = 103
        obj.key = "/test/test1"
        self.reader.read.side_effect = [etcd.EtcdWatchTimedOut(60), obj]
        self.writer.write.return_value = obj.modifiedIndex
        self.assertEqual(self.rc.read_write(102), obj.modifiedIndex)
        self.reader.read.assert_has_calls([mock.call(102), mock.call(102)])
