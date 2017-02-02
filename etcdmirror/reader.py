class Etcd2Reader(object):

    def __init__(self, client, prefix):
        self.prefix = prefix
        self.client = client

    def all_objects(self):
        return self.client.read(self.prefix, recursive=True)

    def read(self, idx):
        return self.client.read(self.prefix, recursive=True, wait=True,
                                waitIndex=idx, timeout=0)
