import datajoint as dj
from connects_local_pipeline_runner.abstracted import Keys
from connects_local_pipeline_runner import plumbing, plumbingtest
import time
slp = dj.create_virtual_module('plumbingtest', 'nihil_m35plumbingtest')

key = {"sleep_time": 10}
hashed_key = Keys().include(key)

# delete keys and repopulate
(plumbing.Jobs.JobAssignment() & hashed_key).delete(force = True)
plumbing.Jobs().initialize('jrK8s')
(plumbing.Jobs() & 'scheme = "test"').assign(hashed_key)



(plumbing.Jobs() & hashed_key).prime()                                         
plumbing.Jobs.Launched.populate(hashed_key)                                            
to_do = ((plumbing.Jobs & 'scheme = "test"') * (plumbing.Jobs.Ready() - plumbing.Jobs.Complete())) & hashed_key
while to_do:
    n_assigned = len((plumbing.Jobs & 'scheme = "test"') * plumbing.Jobs.JobAssignment() & hashed_key)
    n_complete = len((plumbing.Jobs & 'scheme = "test"') * plumbing.Jobs.Complete() & hashed_key)
    n_launched = len((plumbing.Jobs & 'scheme = "test"') * (plumbing.Jobs.Launched() - plumbing.Jobs.Complete()) & hashed_key)
    n_ready = len((plumbing.Jobs & 'scheme = "test"') * (plumbing.Jobs.Ready() - plumbing.Jobs.Launched() - plumbing.Jobs.Complete()) & hashed_key)
    n_queued = len((plumbing.Jobs & 'scheme = "test"') * (plumbing.Jobs.JobAssignment() - plumbing.Jobs.Ready() - plumbing.Jobs.Launched() - plumbing.Jobs.Complete()) & hashed_key)
    print(f'Jobs progress: \n {n_assigned} assigned \n {n_queued} queued \n {n_ready} ready \n {n_launched} launched \n {n_complete} complete (including errors)')
    print("Do not exit until queue/ready is empty.")
    plumbing.Jobs.Launched.populate(to_do)
    time.sleep(20)
    to_do = ((plumbing.Jobs & 'scheme = "test"') * (plumbing.Jobs.Ready() - plumbing.Jobs.Complete())) & hashed_key

print(slp.Sleep())
print(slp.SleepMemory())
print('Done.')