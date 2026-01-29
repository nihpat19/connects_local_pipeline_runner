import datajoint as dj

import numpy as np
import run_neurd_pipeline
import math

m35d = dj.create_virtual_module('minnie35_download','nihil_minnie35_download')
m35p = dj.create_virtual_module('minnie35_process', 'nihil_minnie35_process')
segments_to_proofread = ((m35d.DecimatedMesh * m35d.SomaInfo - m35p.AutoProofreadNeuron) & 'n_somas=1').fetch('segment_id')
batch_size = 1000
num_batches = math.ceil(len(segments_to_proofread)/batch_size)
segments_to_proofread_splits = np.array_split(segments_to_proofread,num_batches)

run_neurd_pipeline.run_segments(segments_to_proofread_splits[0].tolist(),delete_existing_jobs=True)