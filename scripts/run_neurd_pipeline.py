import datajoint as dj
from connects_aws_pipeline_runner.abstracted import Keys
from connects_aws_pipeline_runner import plumbing
import sys
import time

dj.config['safemode'] = False # deletes without prompt
plumbing.load_secret('neurd-dev')
hp = dj.create_virtual_module('h01_process', 'h01_process')

def run_segments(segment_ids, delete_existing_jobs = True):
    if type(segment_ids) is not list:
        segment_ids = list(segment_ids)
    keys = [{'segment_id': segment_id} for segment_id in segment_ids]
    hashed_keys = [Keys().include(key) for key in keys]
    if delete_existing_jobs:
        print('deleting existing assigned jobs from plumbing.Jobs.JobAssignment...')
        (plumbing.Jobs.JobAssignment() & hashed_keys).delete(force = True)
    (plumbing.Jobs() & 'scheme = "connects-aws"').assign(hashed_keys)
    (plumbing.Jobs() & hashed_keys).prime()
    plumbing.Jobs.Launched.populate(hashed_keys)
    to_do = ((plumbing.Jobs & 'scheme = "connects-aws"') * (plumbing.Jobs.Ready() - plumbing.Jobs.Complete())) & hashed_keys
    while to_do:
        n_assigned = len((plumbing.Jobs & 'scheme = "connects-aws"') * plumbing.Jobs.JobAssignment() & hashed_keys)
        n_complete = len((plumbing.Jobs & 'scheme = "connects-aws"') * plumbing.Jobs.Complete() & hashed_keys)
        n_launched = len((plumbing.Jobs & 'scheme = "connects-aws"') * (plumbing.Jobs.Launched() - plumbing.Jobs.Complete()) & hashed_keys)
        n_ready = len((plumbing.Jobs & 'scheme = "connects-aws"') * (plumbing.Jobs.Ready() - plumbing.Jobs.Launched() - plumbing.Jobs.Complete()) & hashed_keys)
        n_queued = len((plumbing.Jobs & 'scheme = "connects-aws"') * (plumbing.Jobs.JobAssignment() - plumbing.Jobs.Ready() - plumbing.Jobs.Launched() - plumbing.Jobs.Complete()) & hashed_keys)
        print(f'Jobs progress: \n {n_assigned} assigned \n {n_queued} queued \n {n_ready} ready \n {n_launched} launched \n {n_complete} complete (including errors)')
        print("Do not exit until queue/ready is empty.")
        plumbing.Jobs.Launched.populate(to_do)
        time.sleep(20)
        delete_multiple_lines(n=7)
        to_do = ((plumbing.Jobs & 'scheme = "connects-aws"') * (plumbing.Jobs.Ready() - plumbing.Jobs.Complete())) & hashed_keys
    
    print('Done.')
    for segment_id in segment_ids:
        status = check_status(segment_id)
        print(f'{segment_id}: {status}')

def check_status(segment_id):
    jobs_table_keys = check_segments_against_jobs_table(segment_id)
    if jobs_table_keys:
        status = jobs_table_keys.fetch('status')
        if len(status) > 1:
            n_errors = len([s for s in status if s == 'error'])
            n_reserved = len([s for s in status if s == 'reserved'])
            status = f"{n_reserved} reserved splits; {n_errors} error splits"
    else: # check to see if completed
        key = {'segment_id': segment_id}
        segment_query = hp.AutoProofreadNeuron & key
        if segment_query:
            status = f"complete; {len(segment_query)} splits"
        else:
            status = 'unknown' # shouldn't happen
    return status
       
def check_segments_against_jobs_table(segment_id):
    jobs = hp.schema.jobs.fetch(as_dict = True)
    matching_jobs = []
    for j in jobs:
        if j['key']['segment_id'] == segment_id:
            matching_jobs.append({'table_name': j['table_name'], 'key_hash':j['key_hash']})
    return hp.schema.jobs & matching_jobs

def delete_multiple_lines(n=1):
    """Delete the last line in the STDOUT."""
    for _ in range(n):
        sys.stdout.write("\x1b[1A")  # cursor up one line
        sys.stdout.write("\x1b[2K")  # delete the last line

if __name__ == "__main__":
    segment_ids = [int(arg) for arg in sys.argv[1:]]
    run_segments(segment_ids)