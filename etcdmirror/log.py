# This file is mostly copied from pybal
import logging
import sys

from twisted.python import log as tw_log
from twisted.python import util


class LogObserver(tw_log.FileLogObserver):
    """Simple log observer derived from FileLogObserver"""
    level = logging.INFO

    def __init__(self, f):
        tw_log.FileLogObserver.__init__(self, f)

    def __call__(self, eventDict):
        if eventDict.get('logLevel', logging.DEBUG) >= self.level:
            return self.emit(eventDict)

    def emit(self, eventDict):
        text = tw_log.textFromEventDict(eventDict)
        if text is None:
            return

        fmtDict = {'system': eventDict['system'],
                   'text': text.replace("\n", "\n\t")}
        msgStr = tw_log._safeFormat("[%(system)s] %(text)s\n", fmtDict)
        util.untilConcludes(self.write, msgStr)
        util.untilConcludes(self.flush)


class Logger(object):
    """Simple logger class that mimics the syntax of normal python logging"""
    levels = {
        logging.DEBUG: 'DEBUG',
        logging.INFO: 'INFO',
        logging.WARN: 'WARN',
        logging.ERROR: 'ERROR',
        logging.CRITICAL: 'CRITICAL'
    }

    def __init__(self, observer):
        tw_log.addObserver(observer)
        for k, v in self.levels.items():
            method_name = v.lower()
            setattr(self,
                    method_name,
                    self._genLogger(k))

    @staticmethod
    def _to_str( level):
        return Logger.levels.get(level, 'DEBUG')

    def _genLogger(self, lvl):
        def _log(msg, *args, **kwargs):
            level = Logger._to_str(lvl)
            sys = kwargs.get('system', 'etcd-mirror')
            msg = msg % tuple(args)
            message = "%s: %s" % (level, msg)
            tw_log.msg(message, logLevel=lvl, system=sys)
        return _log

    @staticmethod
    def err(*args, **kw):
        return tw_log.err(*args, **kw)

stderr = LogObserver(sys.stderr)
log = Logger(observer=stderr)
