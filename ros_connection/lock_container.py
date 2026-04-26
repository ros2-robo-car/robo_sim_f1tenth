import threading
import copy

class lock_list:
    _data = None
    _lock = threading.Lock()

    def __init__(self, len, init_value=0.):
        self._data = [init_value] * len

    def get(self):
        self._lock.acquire()
        ret = copy.deepcopy(self._data)
        self._lock.release()
        return ret
    
    def set(self, data):
        self._lock.acquire()
        self._data = copy.deepcopy(data)
        self._lock.release()