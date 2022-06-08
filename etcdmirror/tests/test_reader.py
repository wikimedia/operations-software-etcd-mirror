import unittest

import etcd
import mock

from etcdmirror import reader


class TestEtcd2Reader(unittest.TestCase):
    def setUp(self):
        self.client = mock.MagicMock(autospec=etcd.Client)
        self.reader = reader.Etcd2Reader(self.client, "/test")

    def test_all_objects(self):
        """
        Test the query for all objects under the current prefix
        """
        self.reader.all_objects()
        self.client.read.assert_called_with("/test", recursive=True)

    def test_read(self):
        """
        Test reading from the source
        """
        self.reader.read(1234)
        self.client.read.assert_called_with(
            "/test", wait=True, waitIndex=1234, recursive=True, timeout=60
        )
