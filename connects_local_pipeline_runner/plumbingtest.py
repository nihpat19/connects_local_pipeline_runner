# for testing remote execution on k8s

import datajoint as dj
import time
import numpy as np

schema = dj.Schema('nihil_m35plumbingtest')

@schema
class SleepTimes(dj.Lookup):
    definition = """ 
    sleep_time : int
    """
    contents = [[1],[3],[4],[10]]

@schema
class Sleep(dj.Computed):
    definition = """ # sleeps for n seconds
    -> SleepTimes
    """
    def make(self, key):
        time.sleep(key['sleep_time'])
        self.insert1(key)

@schema
class SleepMemory(dj.Computed):
    definition = """ # instantiates large random matrices
    -> Sleep 
    """
    def make(self, key):
        n = key['sleep_time']
        np.random.randn(n*100, n*100, n*100)
        self.insert1(key)