import datajoint as dj
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import numpy as np
import math
import run_neurd_pipeline
v1d = dj.create_virtual_module('v1dd_download','nihil_v1dd_download')
v1p = dj.create_virtual_module('v1dd_process', 'nihil_v1dd_process')

# undecimated_segments = (v1d.DownloadedMesh - v1p.MeshDecimation).fetch('segment_id')
# decimated_segments_to_proofread = ((v1p.MeshDecimation - v1p.AutoProofreadNeuron) & 'segment_id != 864691132641863846'
#                                    & 'segment_id != 864691132636068366' & 'segment_id != 864691132650732403').fetch('segment_id')
#for segments in segments_to_proofread_splits:

all_remaining_segments = ((v1d.DownloadedMesh - v1p.SegmentBlacklist - v1p.AutoProofreadNeuron)).fetch('segment_id')
currently_running_segments = np.array([key['segment_id'] for key in (run_neurd_pipeline.check_segments_against_jobs_table(all_remaining_segments.tolist()) & 'status="reserved"').fetch('key')])
all_segments_to_proofread = np.setdiff1d(all_remaining_segments, currently_running_segments)
batch_size = 20000
num_batches = math.ceil(len(all_segments_to_proofread)/batch_size)
segments_to_proofread_splits = np.array_split(all_segments_to_proofread,num_batches)
for batch in segments_to_proofread_splits:
    run_neurd_pipeline.run_segments(batch.tolist(),delete_existing_jobs=True)

