import time
import datajoint as dj
from datajoint.hash import key_hash as kh
from connects_local_pipeline_runner import abstracted, monitoring, clusters
import sys
sys.path.append('/home/nihil/Documents/connects_h01')
import minnie35download
schema = dj.Schema("nihil_m35plumbing")

from getpass import getuser
from importlib import import_module
import os
#from IPython.display import clear_output
ModularTables = abstracted.ModularTables
Keys = abstracted.Keys
ResourceMonitorSimple = monitoring.ResourceMonitorSimple
import datajoint as dj
#m35d = dj.create_virtual_module('minnie35_download','nihil_minnie35_download')

@schema
class JobScheme(dj.Lookup):                                       # TODO: optimization --- make into computed table; calculate ordering, etc.
    definition = """                                           # Collection of schemas we want to populate, with optimization for parallelism
    scheme : varchar(16)
    ---
    height : smallint                                          # longest longest chain of tables that must be populated in sequence
    width : smallint                                           # largest antichain of tables that can be processed in parallel for a given base key, assuming no key splits
    """
    contents = [['connects', 4, 1],
                #['connects-aws', 4, 1],
                ['test', 2, 1]]
    class DataBase(dj.Lookup, dj.Part):    
        definition = """ # Database where tables live
        -> JobScheme
        ---
        database_url : varchar(255)
        """
        contents = [['connects', "jr-database.ad.bcm.edu"],
                    #['connects-aws', "neurd-datajoint.cluster-cjc6cqmcqirl.us-east-1.rds.amazonaws.com"],
         ['test', "jr-database.ad.bcm.edu"]]
    class Tables(dj.Part, dj.Lookup, ModularTables):                    # TODO: topological order and check for missing tables
        definition = """                                       # tables in scheme, listed in topological order and 
        -> JobScheme
        -> abstracted.Table.Modular
        ---
        topological_index : int                                # height
        """
        contents = [['test', 'plumbingtest', 'Sleep', 1],
                    ['test', 'plumbingtest', 'SleepMemory', 2],
                    ['connects', 'minnie35process', 'SomaExtraction', 1],
                    ['connects', 'minnie35process', 'Decomposition', 2],
                    ['connects', 'minnie35process', 'DecompositionCellType', 3],
                    ['connects', 'minnie35process', 'AutoProofreadNeuron', 4],
                    # ['connects-aws', 'minnie35process', 'SomaExtraction', 1],
                    # ['connects-aws', 'minnie35process', 'Decomposition', 2],
                    # ['connects-aws', 'minnie35process', 'DecompositionCellType', 3],
                    # ['connects-aws', 'minnie35process', 'AutoProofreadNeuron', 4],
                    ]
    class Images(dj.Part, dj.Lookup):
        definition = """
        -> JobScheme.Tables
        ---
        -> clusters.Image                                               # docker image on which to run this table
        """

@schema
class Resources(dj.Lookup):
    definition = """
    resource_group : varchar(16)
    ---
    memory = 14 : smallint                                  # in GB
    mem_limit = 32 : smallint
    n_cpus = 3.5 : float
    n_gpus = 0 : tinyint
    gpu_limit = 0 : tinyint
    storage = 25 : int                                      # in GB
    """
    contents = [['r6g.xlarge',None, None, None, None, None, None],
                ['r6g.large', 10, 20, 0.7, 0, 0, 25]]

@schema
class ResourceModel(dj.Lookup):
    definition = """                                      # models resources to request
    model_name: varchar(32)
    """
    contents = [['test'], ['neurd-soma-low'], ['neurd']]

    def model(self, model, key_hash, table):
       if model == 'test':
           return 'r6g.large' if table == 'Sleep' else 'r6g.xlarge'
       if model == 'neurd-soma-low':
           return 'r6g.large' if table == 'SomaExtraction' else 'r6g.xlarge'
       if model == 'neurd':
            #print(Keys())
            #print((Keys() & f'key_hash="{key_hash}"').key)
            key_segment = (Keys() & f'key_hash="{key_hash}"').key[0]['segment_id']
            #print(key_segment)
            soma_count = (minnie35download.SomaInfo & f'segment_id={key_segment}').fetch1('n_somas')
            if soma_count==1:
                segment_filesize_in_mb = (minnie35download.schema.external['decimated_meshes'] & f'filepath like "%{key_segment}%"').fetch1('size')/1e6
                if segment_filesize_in_mb>20:
                    return 'r6g.xlarge'
                else:
                    return 'r6g.large'
            #print(segment_filesize_in_mb)
            else:
                return 'r6g.xlarge'



@schema
class JobGroups(dj.Computed):
    definition = """                    # runs the resource model and gets the group for each table/key pair
    -> abstracted.Keys
    -> JobScheme.Tables
    -> ResourceModel
    ---
    -> Resources
    """
    def make(self, key):
        key['resource_group'] = ResourceModel().model(key['model_name'], key['key_hash'], key['table_name'])
        self.insert1(key)

@schema
class Jobs(dj.Lookup):                                          # TODO: rewrite to allow comparative tests on same key
    definition = """                                              # populate all tables in a pipeline for a given base key
    -> JobScheme
    ---
    -> monitoring.Monitoring
    -> ResourceModel
    -> clusters.Cluster
    started_at = CURRENT_TIMESTAMP : timestamp
    """
          
    class JobAssignment(dj.Manual, dj.Part, ModularTables):
        definition = """ 
        -> Jobs                                        # Builds manifests from resources available and creates jobs
        -> Resources
        -> abstracted.Keys
        """
        @property
        def key_source(self):
            return (Jobs * Resources * abstracted.Keys - Jobs.Complete) & JobGroups

        @property
        def manifest(self):
            req = (Resources * self).fetch1()
            image_name = (clusters.Image * (Jobs & self)).fetch1('name')
            image_url = (clusters.Cluster() * (Jobs & self) * clusters.Image.Repository & f"name = '{image_name}'").fetch1('url')

            container_name = (clusters.Image & JobScheme.Images * self).fetch1('name').replace('_', '-')
            job = load_job_template()
            job['metadata']['name'] = self.job_name
            job['spec']['parallelism'] = 1          # self._max_parallel()
            job['spec']['template']['spec']['containers'][0]['name'] = container_name
            job['spec']['template']['spec']['containers'][0]['image'] = image_url
            job['spec']['template']['spec']['containers'][0]['resources']['requests']['cpu'] = req['n_cpus']
            job['spec']['template']['spec']['containers'][0]['resources']['requests']['nvidia.com/gpu'] = req['n_gpus']
            job['spec']['template']['spec']['containers'][0]['resources']['requests']['memory'] = str(req['memory'])+'Gi'
            job['spec']['template']['spec']['containers'][0]['resources']['limits']['memory'] = str(req['mem_limit'])+'Gi'
            job['spec']['template']['spec']['containers'][0]['resources']['limits']['nvidia.com/gpu'] = req['gpu_limit']
            
            job['spec']['template']['spec']['containers'][0]['env'].append({'name': 'MY_KEY', 'value': self.fetch1('key_hash')})
            job['spec']['template']['spec']['containers'][0]['env'].append({'name': 'RES_GROUP', 'value': req['resource_group']})
            job['spec']['template']['spec']['containers'][0]['env'].append({'name': 'DJ_HOST', 'value': (JobScheme.DataBase & self).fetch1('database_url')})
            if req['resource_group']=='r6g.xlarge':
                job['spec']['template']['spec']['affinity']['nodeAffinity'][
                    'preferredDuringSchedulingIgnoredDuringExecution'][0]['preference']['matchExpressions'][0][
                    'values'][0] = 'high'
            return job
        
        @property
        def job_name(self):
            group = self.fetch1('resource_group')
            scheme = self.fetch1('scheme')
            init_key = (Keys() & self).key              #unhashed key

            # TODO: fix so this works for all cases
            unpacked_values = [str(list(d.values())[0]) for d in init_key]
            job_name = '-'.join([group, scheme, *unpacked_values])
            return job_name

        @property
        def job(self):
            namespace = self._get_user()
            return clusters.Cluster() & (Jobs * self).batch_api.read_namespaced_job(name = self.job_name, namespace = namespace)
        
        def _get_user(self):           #can change user to namespace?
            mgmt = (clusters.Cluster & (Jobs * self)).fetch1('cluster_mgmt')
            if mgmt == 'bcm':
                return getuser()
            elif mgmt == 'aws-eks':
                return 'default'

        def launch(self):
            namespace = self._get_user()
            cluster_name = (clusters.Cluster * (Jobs * self)).fetch1('cluster_name')
            cluster = clusters.Cluster() & f"cluster_name = '{cluster_name}'"
            job = cluster.batch_api.create_namespaced_job(
                body=self.manifest,                                    # TODO: maybe make this more programmatic instead of relying on f-string
                namespace=namespace,
                )
            return job

    class Ready(dj.Part, dj.Manual): 
        definition = """                              # Note: this table is filled in __main__
        -> Jobs.JobAssignment
        """

    class Launched(dj.Part, dj.Computed):
        definition = """ # launched k8s jobs
        -> Jobs.Ready
        """
        
        def make(self, key):                          
            self.insert1(key)
            job = (Jobs.JobAssignment() & key).launch()
            self.status = job.status

    class Run(dj.Manual, dj.Part):
        definition = """                                                 # Individual keys that have been run within a job
        -> Jobs.Launched
        -> JobGroups
        ---
        pod_name : varchar(255)
        node_name : varchar(64)
        pod_ip : varchar(16)
        """

    class Stats(dj.Lookup, dj.Part):
        definition = """
        -> Jobs.Run
        ---
        runtime : float
        max_memory = NULL : float
        max_cpu = NULL : float
        mean_cpu = NULL : float
        """
        
    class Complete(dj.Computed, dj.Part, ModularTables):
        definition = """                                                # completed jobs
        -> Jobs.Launched
        ---
        completed_at = CURRENT_TIMESTAMP : timestamp
        """
        def make(self, key):
            print(key)
            print(Jobs.Complete().key_source)
            self.next_jobs(key)
            self.insert1(key)

        def next_jobs(self, key): # sets next job(s) after a job is complete
#            keycopy = key.copy()
            # if keycopy['resource_group'] == 'r6g.large':
            #     keycopy['resource_group'] = 'r6g.xlarge' # temporary hack --- see commented lines below
            #     print('pre-insert to Ready:', keycopy)
            #     Jobs.Ready.insert1(keycopy, skip_duplicates = True)
            # else:
            return

    def initialize(self, cluster, scheme = 'test', monitor = 'simple', resource_model = 'test'):
        self.insert1({'scheme': scheme, 'monitor_name':monitor, 'cluster_name' : cluster, 'model_name' : resource_model}, skip_duplicates = True)
        load_secret(cluster)
        #TODO: spin up EKS nodes
    
    # populate up to JobAssignment
    def assign(self, key):
        #kh = Keys().include(key)          # convert to abstract key
        tables = JobScheme.Tables & self
        print(tables)
        res_model = ((Jobs & key) & ResourceModel())
        print(res_model)
        JobGroups.populate(tables, key, res_model)
        self.JobAssignment.insert((Jobs & key) * JobGroups, ignore_extra_fields = True, skip_duplicates = True)

    def prime(self):
        job1 = JobGroups & (JobScheme.Tables & "topological_index = 1" & self)
        print(job1)
        self.Ready.insert(job1, ignore_extra_fields = True, skip_duplicates = True)

    def run(self):
        to_do = self * (self.Ready() - self.Complete())
        while to_do:
            #clear_output(wait = True)
            n_assigned = len(self.JobAssignment())
            n_complete = len(self.Complete())
            n_launched = len(self.Launched() - self.Complete())
            n_ready = len(self.Ready() - self.Launched() - self.Complete())
            n_queued = len(self.JobAssignment() - self.Ready() - self.Launched() - self.Complete())
            print(f'Jobs progress: \n {n_assigned} assigned \n {n_queued} queued \n {n_ready} ready \n {n_launched} launched \n {n_complete} complete')
            #print(self.Launched.key_source)
            self.Launched.populate(to_do)
            time.sleep(20)
            to_do = self * (self.JobAssignment() - self.Complete())

#TODO: tables to run keys on different job schemes and compare stats

def load_job_template():
    import yaml
    with open("../k8s/job-template.yaml") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

def load_secret(cluster, verbose = False):
    import yaml
    with open("../k8s/populate-service-credentials.yaml") as stream:
        try:
            body = yaml.safe_load(stream)
            api = (clusters.Cluster() & f"cluster_name = '{cluster}'").core_api
            try:
                response = api.read_namespaced_secret('populate-service-credentials', 'nihil')
            except:
                response = api.create_namespaced_secret('nihil', body)

            if verbose:
                print(response)

        except yaml.YAMLError as exc:
            print(exc)


JobScheme.Images.insert(JobScheme.Tables * clusters.Image, skip_duplicates = True, ignore_extra_fields = True)

if __name__ == "__main__":
    # runs in every pod
    key = {'key_hash' : os.environ["MY_KEY"]}

    site_info = dict(
        pod_name = os.environ['MY_POD_NAME'],
        node_name = os.environ['MY_NODE_NAME'],
        pod_ip = os.environ['MY_POD_IP'],

    )

    resource_query = JobGroups & key & {'resource_group': os.environ['RES_GROUP']}
    unhashed_key = (Keys() & key).key
    print('key: ', str(unhashed_key))
    tables = (JobScheme.Tables & resource_query).fetch(as_dict = True, order_by = 'topological_index') 

    print(f"pod at {str(site_info)} populating tables:")
    for table in tables:
        table_name = table['table_name']
        print(table_name)
        unhashed_key = (Keys() & key).key
        monitor_name, monitor_class = (monitoring.Monitoring & Jobs & (Jobs.JobAssignment & key)).fetch1('monitor_name', 'monitor_class')
        monitor = eval(monitor_class)()
        monitor.start()
        (abstracted.Table() & table).obj.populate(unhashed_key, reserve_jobs = True) # TODO: prioritize
        monitor.stop()
        assignment_key = ((JobGroups & key & table) * (Jobs.JobAssignment & key)).fetch1()
        print(assignment_key)
        Jobs.Run.insert1({**assignment_key, **site_info, 'table_name': table_name}, ignore_extra_fields = True,skip_duplicates = True)
        Jobs.Stats.insert1({**assignment_key, **monitor.stats(), 'table_name': table_name}, ignore_extra_fields = True,skip_duplicates = True)
    print(Jobs.Launched & key & {'resource_group': os.environ['RES_GROUP']})
    Jobs.Complete.populate(Jobs.Launched & key & {'resource_group': os.environ['RES_GROUP']})
