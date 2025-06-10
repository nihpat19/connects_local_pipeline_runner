# connects_aws_pipeline_runner

- Allows for NEURD pipeline to be run on multiple clusters, including AWS EKS
- Includes datajoint-native tools for generating and running kubernetes workflows/pipelines
- Maps datajoint tables to docker images and to table-specific resource requests to lower costs
- Some limited monitoring capabilities

## Installation
`pip install git+https://github.com/reimerlab/connects_aws_pipeline_runner.git`

## To run H01 segments on the `neurd-dev` AWS EKS cluster
1. Obtain credentials and save in `k8s/populate-service-credentials.yaml`
2. From scripts directory, run `python run_neurd_pipeline.py [segment1, segment2, ...]`
