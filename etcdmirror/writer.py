import re

import etcd

from etcdmirror.log import log


class Etcd2Writer(object):
    def __init__(self, client, prefix, src_prefix=None, ignore_keys=None):
        if not prefix.startswith("/"):
            raise ValueError("All paths must be absolute: %s", prefix)
        self.prefix = prefix
        self.trimmed_prefix = prefix.rstrip("/")
        if src_prefix is not None:
            if not src_prefix.startswith("/"):
                raise ValueError("All paths must be absolute: %s", src_prefix)
            # If src_prefix is empty, there is no need to attempt to trim it
            # from the src key later on.
            src_prefix = src_prefix.rstrip("/") or None
        self.src_prefix = src_prefix
        self.client = client
        if ignore_keys is None:
            self.ignore_regex = None
        else:
            self.ignore_regex = re.compile(ignore_keys)
        # If trimmed_prefix is empty (full keyspace), use a distinct suffix on
        # the index key. This ensures /__replication is always a directory, and
        # enables switching between full / partial keyspace mirroring without a
        # manual cleanup.
        self.idx = "/__replication" + (self.trimmed_prefix if self.trimmed_prefix else "/__ROOT__")
        log.info("Replication index will be tracked in: %s", self.idx)

    def key_for(self, orig_key):
        if self.src_prefix is None:
            return self.trimmed_prefix + orig_key
        return self.trimmed_prefix + orig_key.replace(self.src_prefix, "", 1)

    def ignore_key(self, orig_key):
        if self.ignore_regex is None:
            return False
        return self.ignore_regex.fullmatch(orig_key) is not None

    def apply_action(self, obj, key):
        log.debug("Event: %s on %s", obj.action, key)
        if obj.action == "create":
            self.client.write(key, obj.value, dir=obj.dir, prevExist=False, ttl=obj.ttl)
        elif obj.action == "compareAndSwap":
            self.client.write(
                key,
                obj.value,
                dir=obj.dir,
                prevValue=obj._prev_node.value,
                ttl=obj.ttl,
            )
        elif obj.action == "set":
            self.client.write(key, obj.value, dir=obj.dir, ttl=obj.ttl)
        elif obj.action == "delete":
            self.client.delete(key, recursive=obj.dir)
        elif obj.action == "expire":
            try:
                self.client.delete(key, recursive=obj.dir)
            except etcd.EtcdKeyNotFound:
                log.info("Not deleting already expired key %s", key)
        else:
            log.warn("Unrecognized action %s, skipping", obj.action)

    def write(self, obj):
        idx = obj.modifiedIndex
        if self.ignore_key(obj.key):
            log.debug("Ignoring operation on %s", obj.key)
        else:
            key = self.key_for(obj.key)
            try:
                self.apply_action(obj, key)
            except Exception as e:
                log.error("Action %s failed on %s: %s", obj.action, key, e)
                return False
        # If this causes an exception, we don't catch it on purpose
        self.client.write(self.idx, idx, prevExist=True)
        return obj.modifiedIndex

    def load_from_dump(self, rootobj):
        for obj in rootobj.leaves:
            if obj.key is None:
                continue
            if self.ignore_key(obj.key):
                log.debug("Ignoring %s during loading", obj.key)
            else:
                log.debug("Loading %s", obj.key)
                key = self.key_for(obj.key)
                self.client.write(key, obj.value, dir=obj.dir, ttl=obj.ttl)
        self.client.write(self.idx, rootobj.etcd_index)

    def cleanup(self):
        try:
            self.client.delete(self.idx)
        except etcd.EtcdKeyNotFound:
            log.info("Could not find %s, assuming new replica", self.idx)
        except Exception as error:
            log.info("Error removing the replication key, giving up: %s", error)
            return False
        try:
            self.client.delete(self.prefix, recursive=True)
        except etcd.EtcdKeyNotFound:
            log.info("Key %s not found, not cleaning up", self.prefix)
        except etcd.EtcdRootReadOnly:
            for obj in self.client.read(self.prefix).leaves:
                if obj.key is None:
                    continue
                log.debug("Removing %s", obj.key)
                self.client.delete(obj.key, recursive=obj.dir)
        except Exception as error:
            log.error("Error removing the replica directory: %s", error)
            return False
        return True
