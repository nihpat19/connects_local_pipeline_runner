import datajoint as dj
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import numpy as np

import run_neurd_pipeline
m35d = dj.create_virtual_module('minnie35_download','nihil_minnie35_download')
v1p = dj.create_virtual_module('v1dd_process', 'nihil_v1dd_process')

segments_to_proofread_splits = (v1p.MeshDecimation - v1p.AutoProofreadNeuron).fetch('segment_id')
for segments in segments_to_proofread_splits:
    run_neurd_pipeline.run_segments(segments.tolist(),delete_existing_jobs=True)

