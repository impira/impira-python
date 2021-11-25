from ..config import get_logger


class Tool(object):
    def _log(self):
        if not (hasattr(self, "_cached_log") and self._cached_log is not None):
            self._cached_log = get_logger(type(self).__name__.lower())
        return self._cached_log
