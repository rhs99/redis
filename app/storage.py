import time
import threading


class Storage:
    def __init__(self):
        # self.mutex = threading.Lock()
        self.data = dict()
        self.expiry = dict()
        self.stream = set()

    def get(self, key):
        # self.mutex.acquire()

        if (
            key in self.expiry
            and self.expiry[key] <= time.time() * 1000
        ):
            self.data.pop(key)
            self.expiry.pop(key)

        val = self.data.get(key, None)

        # self.mutex.release()

        return val

    def set(self, key, val, exp=None):
        # self.mutex.acquire()

        self.data[key] = val
        if exp:
            self.expiry[key] = time.time() * 1000 + float(
                exp
            )

        # self.mutex.release()
