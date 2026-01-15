"""
Some code taken from:
https://medium.com/@aris.david/how-to-create-a-job-using-kubernetes-python-client-ed00ac2b791d
"""

import datajoint as dj
import logging
from pathlib import Path
#logging.basicConfig(level=logging.INFO)
try:
    import kubernetes
except:
    print("No EKS or kubernetes control plane support.")

import base64

schema = dj.Schema('nihil_m35clusters')

@schema
class ClusterManagement(dj.Lookup):
    definition = """
    cluster_mgmt : varchar(32)                                             # Cluster management vendor/service
    """
    contents = [['bcm']]

@schema
class Region(dj.Lookup):
    definition = """
    -> ClusterManagement
    region : varchar(16)                                                   # region ('local' for local clusters)
    """
    contents = [['bcm', 'local'], 
                ]

@schema
class Image(dj.Lookup):
    definition = """     
    name : varchar(255)                                                     # shorthand identifier
    ---
    tag : varchar(64)                                                      # image tag
    """
    contents = [['neurd_v2p', 'minnie35']]
    class Repository(dj.Lookup, dj.Part):
        definition = """                                                   # URLs for pulling docker images
        -> Image
        -> Region
        ---
        url : varchar(255)                                                 # location of repository
        """
        contents = [
            ['neurd_v2p',
            'bcm', 
            'local', 
            'jr-saltmaster.ad.bcm.edu:5000/neurd_v2p:minnie35'],
            # ['neurd_v2p',
            #   'aws-eks',
            #   'us-east-1',
            #   '345594566610.dkr.ecr.us-east-1.amazonaws.com/neurd/v2:latest'
            #  ]
             ]
    def sync_images(self):
        #TODO: sync all local and eks images
        pass

@schema
class Cluster(dj.Lookup):
    definition = """
    cluster_name : varchar(255)
    ---
    -> Region
    """
    contents = [['jrk8s', 'bcm', 'local'],
                #['neurd-dev', 'aws-eks', 'us-east-1']
                ]

    @property
    def client(self):
        #cluster_name, cluster_mgmt, region = self.fetch1('cluster_name', 'cluster_mgmt', 'region')
        #if cluster_mgmt == 'bcm':
        return self._set_client_local()
        #if cluster_mgmt == 'aws-eks':
            #return self._set_client_remote(cluster_name, region)
    # @property
    # def eks_client(self):

    @property
    def core_api(self):
        return kubernetes.client.CoreV1Api(self.client)
        
    @property
    def batch_api(self):
        return kubernetes.client.BatchV1Api(self.client)
    
    def _set_client_local(self):
        #kubernetes.config.load_kube_config('/home/nihil/.kube/nihil.kubeconfig')
        configuration = kubernetes.client.Configuration()
        configuration.host = r"https://10.28.0.136:6443"
        configuration.ssl_ca_cert = "/usr/local/share/ca-certificates/k8s-ca.crt"
        configuration.cert_file = "/usr/local/share/ca-certificates/client.crt"
        configuration.key_file = "../../client.key"
        configuration.verify_ssl = False
        return kubernetes.client.ApiClient(configuration)


    # def _set_client_remote(self, cluster_name, region):
    #     token, endpoint, cert_authority_data = self.get_eks_credentials(cluster_name, region)
    #     _client = kubernetes.client
    #     # Save the CA certificate in a temporary file
    #     with open(Path('ca.crt'), 'w') as f:
    #         f.write(base64.b64decode(cert_authority_data).decode('utf-8'))
    #
    #     # Create Kubernetes client configuration using the token and cluster details
    #     configuration = _client.Configuration()
    #     configuration.host = endpoint
    #     configuration.verify_ssl = True
    #     configuration.ssl_ca_cert = Path('ca.crt')
    #     configuration.api_key = {"authorization": f"Bearer {token}"}
    #
    #     # Set the configuration for the Kubernetes client
    #     _client.Configuration.set_default(configuration)
    #     configuration = _client.Configuration.get_default_copy()
    #     return kubernetes.client.ApiClient(configuration=configuration)
        #return _client


    # def get_eks_credentials(self, cluster_name, region):
    #     """Get EKS token, endpoint, and certificate authority data."""
    #     session = boto3.Session()
    #     eks_client = session.client("eks", region_name=region)
    #
    #     # Get cluster info (API server endpoint and certificate authority data)
    #     cluster_info = eks_client.describe_cluster(name=cluster_name)["cluster"]
    #     endpoint = cluster_info["endpoint"]
    #     cert_authority_data = cluster_info["certificateAuthority"]["data"]
    #
    #     # Get authentication token using the eks_token
    #     token = get_token(cluster_name=cluster_name)['status']['token']
    #
    #     return token, endpoint, cert_authority_data


    def create_namespace(self, namespace):

        namespaces = self.core_api.list_namespace()
        all_namespaces = []
        for ns in namespaces.items:
            all_namespaces.append(ns.metadata.name)

        if namespace in all_namespaces:
            logging.info(f"Namespace {namespace} already exists. Reusing.")
        else:
            namespace_metadata = kubernetes.client.V1ObjectMeta(name=namespace)
            self.core_api.create_namespace(
                kubernetes.client.V1Namespace(metadata=namespace_metadata)
            )
            logging.info(f"Created namespace {namespace}.")

        return namespace

    def create_container(self, image, name, pull_policy, args):

        container = self.client.V1Container(
            image=image,
            name=name,
            image_pull_policy=pull_policy,
            args=[args],
            command=["python3", "-u", "./shuffler.py"],
        )

        logging.info(
            f"Created container with name: {container.name}, "
            f"image: {container.image} and args: {container.args}"
        )

        return container

    def create_pod_template(self, pod_name, container):
        pod_template = self.client.V1PodTemplateSpec(
            spec=self.client.V1PodSpec(restart_policy="Never", containers=[container]),
            metadata=self.alterclient.V1ObjectMeta(name=pod_name, labels={"pod_name": pod_name}),
        )

        return pod_template

    def create_job(self, job_name, pod_template):
        metadata = self.client.V1ObjectMeta(name=job_name, labels={"job_name": job_name})

        job = self.batch_api.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=metadata,
            spec=self.batch_api.V1JobSpec(backoff_limit=0, template=pod_template),
        )

        return job
    def node_labels(self):
        # Listing the cluster nodes
        node_list = self.core_api.list_node()
        # Patching the node labels
        for node in node_list.items:
            print(f"{node.metadata.name}\t{node.metadata.labels}")