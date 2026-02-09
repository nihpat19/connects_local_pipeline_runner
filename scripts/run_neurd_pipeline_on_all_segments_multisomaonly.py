import datajoint as dj
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import math
import numpy as np
import run_neurd_pipeline

m35d = dj.create_virtual_module('minnie35_download','nihil_minnie35_download')
m35p = dj.create_virtual_module('minnie35_process', 'nihil_minnie35_process')
segments_to_proofread = ((m35d.SomaInfo & 'n_somas>1') - m35p.AutoProofreadNeuron - (m35p.schema.jobs & 'status="reserved"').fetch('key')).fetch('segment_id')
# batch_size = 1000
# num_batches = math.ceil(len(segments_to_proofread)/batch_size)
# segments_to_proofread_splits = np.array_split(segments_to_proofread,num_batches)
# for segments in segments_to_proofread_splits:
run_neurd_pipeline.run_segments(segments_to_proofread,delete_existing_jobs=True)

