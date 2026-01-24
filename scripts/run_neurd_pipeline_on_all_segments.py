import datajoint as dj
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import numpy as np

import run_neurd_pipeline
m35d = dj.create_virtual_module('minnie35_download','nihil_minnie35_download')
m35p = dj.create_virtual_module('minnie35_process', 'nihil_minnie35_process')

segments_to_proofread_splits = np.array_split((m35d.Segment - m35p.AutoProofreadNeuron).fetch('segment_id'), 300)
for segments in segments_to_proofread_splits:
    run_neurd_pipeline.run_segments(segments.tolist(),delete_existing_jobs=True)

