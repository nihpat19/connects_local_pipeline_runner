from setuptools import setup, find_packages

def parse_requirements(filename):
    with open(filename, 'r') as f:
        lines = f.read().splitlines()
    # Filter out comments and empty lines
    return [line for line in lines if line and not line.startswith('#')]

setup(
    name='connects_aws_pipeline_runner',
    version='0.1.0',
    packages=find_packages(),
    install_requires=parse_requirements('requirements.txt'),
    extras_require={
    },
    author='Robert Law',
    description='Datajoint-native tools for running the NEURD pipeline on AWS EKS clusters',
    url='https://github.com/reimerlab/connects_aws_pipeline_runner',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)