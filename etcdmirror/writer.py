import etcd

from etcdmirror.log import log


class Etcd2Writer(object):

    def __init__(self, client, prefix, src_prefix=None):
        log.info(prefix)
        self.prefix = prefix
        if src_prefix is not None and src_prefix.endswith("/"):
            src_prefix = src_prefix[:-1]
        self.src_prefix = src_prefix
        self.client = client
        self.idx = '/__replication' + self.prefix

    def key_for(self, orig_key):
        if self.src_prefix is None:
            return orig_key
        return self.prefix + orig_key.replace(self.src_prefix, '', 1)

    def write(self, obj):
        idx = obj.modifiedIndex
        key = self.key_for(obj.key)
        try:
            log.debug("Event: %s on %s", obj.action, key)
            if obj.action == 'create':
                self.client.write(
                    key, obj.value,
                    dir=obj.dir, prevExist=False,
                    ttl=obj.ttl)
            elif obj.action == 'compareAndSwap':
                self.client.write(
                    key, obj.value, dir=obj.dir,
                    prevValue=obj._prev_node.value,
                    ttl=obj.ttl)
            elif obj.action == 'set':
                self.client.write(key, obj.value, dir=obj.dir,
                                  ttl=obj.ttl)
            elif obj.action == 'delete':
                self.client.delete(key, recursive=obj.dir)
            elif obj.action == 'expire':
                try:
                    self.client.delete(key, dir=obj.dir, recursive=obj.dir)
                except etcd.EtcdKeyNotFound:
                    log.info("Not deleting already expired key %s", key)
            else:
                log.warn("Unrecognized action %s, skipping", obj.action)
        except Exception as e:
            log.error("Action %s failed on %s: %s", obj.action,
                      key, e)
            return False
        else:
            # If this causes an exception, we don't catch it on purpose
            self.client.write(self.idx, idx, prevExist=True)
            return obj.modifiedIndex

    def load_from_dump(self, rootobj):
        for obj in rootobj.leaves:
            if obj.key is None:
                continue
            log.debug("Loading %s", obj.key)
            key = self.key_for(obj.key)
            self.client.write(
                key, obj.value,
                dir=obj.dir,
                ttl=obj.ttl)
        self.client.write(self.idx, rootobj.etcd_index)

    def cleanup(self):
        try:
            self.client.delete(self.prefix, recursive=True)
        except etcd.EtcdKeyNotFound:
            log.info("Key %s not found, not cleaning up", self.prefix)
            return True
        except etcd.EtcdRootReadOnly:
            self.client.delete(self.idx)
            for obj in self.client.read(self.prefix).leaves:
                if obj.key is None:
                    continue
                log.debug("Removing %s", obj.key)
                self.client.delete(obj.key, recursive=obj.dir)

            return True
        else:
            self.client.delete(self.idx)
            return True
