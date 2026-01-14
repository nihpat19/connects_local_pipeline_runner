import datajoint as dj
#from scalene import scalene_profiler
schema = dj.Schema('nihil_m35monitoring')
import time

class ResourceMonitor:
    prefix = ""
    postfix = ""
    # @abstractmethod
    # def start(self):
    #     pass
    # @abstractmethod
    # def stop(self):
    #     pass
    # @abstractmethod
    # def stats(self):
    #     pass

class ResourceMonitorSimple(ResourceMonitor):
    def start(self):
        self.start_time = time.time()
    def stop(self):
        self.stop_time = time.time()
    def stats(self):
        return {'runtime': self.stop_time - self.start_time}

@schema
class Monitoring(dj.Lookup):
    definition = """
    monitor_name : varchar(64)
    ---
    monitor_class : varchar(64)                                    # class name in this directory
    """
    contents = [["simple", "ResourceMonitorSimple"]]
    