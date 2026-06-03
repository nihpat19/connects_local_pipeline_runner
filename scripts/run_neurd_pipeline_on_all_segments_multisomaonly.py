import datajoint as dj
import os
import sys
#sys.path.append('../../connects_local_pipeline_runner')
import math
import numpy as np
import run_neurd_pipeline
import pandas as pd

v1d = dj.create_virtual_module('v1dd_download','nihil_v1dd_download')
v1p = dj.create_virtual_module('v1dd_process', 'nihil_v1dd_process')
remaining_unclassified_segments = pd.read_csv('../remaining_unclassified_segments.csv',index_col=0)['segment_id'].values
currently_running_and_failed_segments = np.array([key['segment_id'] for key in
                                                  (run_neurd_pipeline.check_segments_against_jobs_table(remaining_unclassified_segments.tolist())).fetch('key')])
segments_to_proofread = np.setdiff1d(remaining_unclassified_segments,currently_running_and_failed_segments)

run_neurd_pipeline.run_segments(segments_to_proofread.tolist(),delete_existing_jobs=True)

