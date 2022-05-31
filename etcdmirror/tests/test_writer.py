import unittest

import etcd
import mock

from etcdmirror import writer


class TestEtcd2Writer(unittest.TestCase):
    def setUp(self):
        self.client = mock.MagicMock(autospec=etcd.Client)
        self.writer = writer.Etcd2Writer(self.client, "/test")

    def test_init(self):
        """
        Test object initialization
        """
        self.assertEqual(self.client, self.writer.client)
        self.assertEqual(self.writer.prefix, "/test")
        self.assertEqual(self.writer.src_prefix, None)
        self.assertEqual(self.writer.idx, "/__replication/test")
        # If path is not absolute, an exception is raised
        with self.assertRaises(ValueError):
            writer.Etcd2Writer(self.client, "test")
        # If src_prefix is present and ends with a slash, that's removed
        w = writer.Etcd2Writer(self.client, "/test", "/test2/")
        self.assertEqual(w.src_prefix, "/test2")
        # If src_prefix is not an absolute path, an exception is raised
        with self.assertRaises(ValueError):
            writer.Etcd2Writer(self.client, "/test", "test2/")

    def test_key_for(self):
        """
        Test key produced by the `key_for` method
        """
        # No src_prefix
        self.assertEqual(self.writer.key_for("/example/key"), "/test/example/key")
        # With src_prefix
        w = writer.Etcd2Writer(self.client, "/test", "/example")
        self.assertEqual(w.key_for("/example/key"), "/test/key")

    def test_write(self):
        obj = mock.MagicMock()
        obj.key = "/some/key"
        obj.action = "create"
        obj.value = "somevalue"
        obj.dir = False
        obj.ttl = 15
        obj.modifiedIndex = 1234
        res = self.writer.write(obj)
        self.assertEqual(res, 1234)
        self.client.write.assert_has_calls(
            [
                mock.call("/test/some/key", "somevalue", dir=False, prevExist=False, ttl=15),
                mock.call("/__replication/test", 1234, prevExist=True),
            ]
        )
        self.client.write.reset_mocks()
        obj.action = "compareAndSwap"
        obj._prev_node.value = "someothervalue"
        self.writer.write(obj)
        self.client.write.assert_any_call(
            "/test/some/key", "somevalue", prevValue="someothervalue", ttl=15, dir=False
        )
        self.client.write.reset_mocks()
        obj.action = "set"
        self.writer.write(obj)
        self.client.write.assert_any_call("/test/some/key", "somevalue", ttl=15, dir=False)
        obj.action = "delete"
        self.writer.write(obj)
        self.client.delete.assert_any_call("/test/some/key", recursive=False)
        # Expire
        obj.action = "expire"
        self.writer.write(obj)
        self.client.delete.assert_any_call("/test/some/key", recursive=False)
        # Race condition: the key has already expired
        self.client.delete.reset_mocks()
        self.client.write.reset_mocks()
        self.client.delete.side_effect = etcd.EtcdKeyNotFound("/test/some/key")
        # Does not raise an exception
        self.writer.write(obj)
        # The index will be written anyways
        self.client.write.assert_called_with("/__replication/test", 1234, prevExist=True)
        obj.action = "abracadabra"
        self.client.delete.reset_mocks()
        self.client.write.reset_mocks()
        self.assertEqual(self.writer.write(obj), 1234)
        # Now test handling of exceptions
        obj.action = "delete"
        self.assertFalse(self.writer.write(obj))

    def leaves(self, recursive=True):
        obj = mock.Mock()
        obj.key = None
        yield obj
        obj = mock.Mock()
        obj.key = "/my/key"
        obj.dir = True
        obj.value = None
        obj.ttl = None
        yield obj
        if recursive:
            for idx in range(2):
                obj = mock.Mock()
                obj.key = "/my/key/%d" % idx
                obj.modifiedIndex = 1234 + idx
                obj.value = "somevalue%d" % idx
                obj.dir = False
                obj.ttl = None
                yield obj

    def test_load_from_dump(self):
        rootobj = mock.Mock()
        rootobj.etcd_index = 1237
        rootobj.leaves = [leaf for leaf in self.leaves()]
        self.writer.src_prefix = "/my"
        calls = [
            mock.call("/test/key", None, dir=True, ttl=None),
            mock.call("/test/key/0", "somevalue0", dir=False, ttl=None),
            mock.call("/test/key/1", "somevalue1", dir=False, ttl=None),
            mock.call("/__replication/test", 1237),
        ]
        self.writer.load_from_dump(rootobj)
        self.client.write.assert_has_calls(calls)
        self.writer.src_prefix = None

    @mock.patch("etcdmirror.log.log.info")
    def test_cleanup(self, logmocker):
        self.assertTrue(self.writer.cleanup())
        # Check calls
        self.client.delete.assert_has_calls(
            [
                mock.call("/__replication/test"),
                mock.call("/test", recursive=True),
            ]
        )
        logmocker.assert_not_called()
        # When the key is not found, but the idx is
        self.client.delete.reset_mocks()
        self.client.delete = mock.Mock(side_effect=[True, etcd.EtcdKeyNotFound("test")])
        self.assertTrue(self.writer.cleanup())
        self.client.delete.assert_called_with("/test", recursive=True)
        logmocker.assert_called_with("Key %s not found, not cleaning up", "/test")
        # When key is found but the idx is not
        self.client.delete = mock.Mock(side_effect=[etcd.EtcdKeyNotFound("idx"), True])
        self.assertTrue(self.writer.cleanup())
        logmocker.assert_called_with(
            "Could not find %s, assuming new replica", "/__replication/test"
        )

        # When trying to delete root
        obj = mock.Mock()
        leaves = [leaf for leaf in self.leaves(False)]
        obj.leaves = [leaves[0], leaves[1]]
        self.client.read.return_value = obj
        self.client.delete = mock.Mock(side_effect=[True, etcd.EtcdRootReadOnly("test"), True])
        self.assertTrue(self.writer.cleanup())
        self.client.read.assert_called_with("/test")
        self.client.delete.assert_called_with("/my/key", recursive=True)

        # Now let's test errors
        self.client.delete.side_effect = ValueError("meh")
        self.assertFalse(self.writer.cleanup())
        self.client.delete.assert_called_with("/__replication/test")
        self.client.delete.side_effect = [True, ValueError("meh")]
        self.assertFalse(self.writer.cleanup())
        self.client.delete.assert_called_with("/test", recursive=True)
