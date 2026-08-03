import datajoint as dj
dj.config.load('./dj_local_conf.json')
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import numpy as np
import math
import run_neurd_pipeline
ind = dj.create_virtual_module('nihil_diamond_download','nihil_diamond_download')
inp = dj.create_virtual_module('nihil_diamond_process', 'nihil_diamond_process')

# undecimated_segments = (ind.DownloadedMesh - inp.MeshDecimation).fetch('segment_id')
# decimated_segments_to_proofread = ((inp.MeshDecimation - inp.AutoProofreadNeuron) & 'segment_id != 864691132641863846'
#                                    & 'segment_id != 864691132636068366' & 'segment_id != 864691132650732403').fetch('segment_id')
#for segments in segments_to_proofread_splits:
all_segments_with_somas = np.load('./segment_ids_with_somas.npy',allow_pickle=True)
soma_segment_dicts = [dict(segment_id=id) for id in all_segments_with_somas]
all_remaining_segments = ((ind.DecimatedMesh - inp.SegmentBlacklist - inp.AutoProofreadNeuron) & soma_segment_dicts).fetch('segment_id')
currently_running_and_failed_segments = np.array([key['segment_id'] for key in (run_neurd_pipeline.check_segments_against_jobs_table(all_remaining_segments.tolist())).fetch('key')])
all_segments_to_proofread = np.setdiff1d(all_remaining_segments, currently_running_and_failed_segments)

#all_segments_to_proofread = np.setdiff1d(all_segments_to_proofread,unaccounted_keys)
batch_size = 1000
num_batches = math.ceil(len(all_segments_to_proofread)/batch_size)
segments_to_proofread_splits = np.array_split(all_segments_to_proofread,num_batches)
for batch in segments_to_proofread_splits:
    run_neurd_pipeline.run_segments(all_segments_to_proofread.tolist(),delete_existing_jobs=True)

