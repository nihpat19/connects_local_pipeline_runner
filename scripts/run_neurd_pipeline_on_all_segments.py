import datajoint as dj
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import numpy as np

import run_neurd_pipeline
v1d = dj.create_virtual_module('v1dd_download','nihil_v1dd_download')
v1p = dj.create_virtual_module('v1dd_process', 'nihil_v1dd_process')

segments_to_proofread = (v1d.DownloadedMesh - v1p.MeshDecimation).fetch('segment_id')
run_neurd_pipeline.run_segments(segments_to_proofread.tolist(),delete_existing_jobs=True)

