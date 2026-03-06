import datajoint as dj
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import numpy as np
import math

import run_neurd_pipeline
#v1d = dj.create_virtual_module('v1dd_download','nihil_v1dd_download')
v1p = dj.create_virtual_module('v1dd_process', 'nihil_v1dd_process')

segments_to_proofread = (v1p.MeshDecimation - v1p.AutoProofreadNeuron).fetch('segment_id')
# batch_size = 1000
# num_batches = math.ceil(len(segments_to_proofread)/batch_size)
# segments_to_proofread_splits = np.array_split(segments_to_proofread,num_batches)
# for segments in segments_to_proofread_splits:
#     run_neurd_pipeline.run_segments(segments.tolist(),delete_existing_jobs=True)
run_neurd_pipeline.run_segments(segments_to_proofread.tolist(),delete_existing_jobs=True)