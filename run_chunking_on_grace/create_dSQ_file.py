import os
import sys

import pandas as pd

if 'sk2248' in os.getcwd():
    sys.path.extend(r'/gpfs/gibbs/project/kelly/sk2248/CorePython/experiments/')
    sys.path.append(r'/gpfs/gibbs/project/kelly/sk2248/CorePython/experiments/')
    sys.path.extend(r'/gpfs/gibbs/project/kelly/sk2248/ragflow/')
    sys.path.append(r'/gpfs/gibbs/project/kelly/sk2248/ragflow/')
    BASE_FOLDER = r'/vast/palmer/scratch/kelly/sk2248/analyst_reports/'
else:
    BASE_FOLDER = r'E:\analyst_reports'

pdf_folder_name = 'ticker_amaskcd_matched_pdfs_save'
partition_run_folder = os.path.join(BASE_FOLDER, pdf_folder_name + '_partition')

# dsq file for scavenge_gpu
file_name = f"run_ragflow_api_calls.txt"
print('dsq --job-file ' + file_name + ' --cpus-per-task=4 --mem=180G --time=1-00:00:00 --output=JOBLOG_ragflow_parsing/dsq-jobfile-%A_%a-%N.out -p scavenge')
file1 = open(file_name, "w")
for parid_file in os.listdir(partition_run_folder):
    parid = parid_file.split('_')[1]
    # line = f"export HF_ENDPOINT=https://hf-mirror.com; export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK; " \
    #        f"module load miniconda; conda activate ragflow; python ragflow_chunking.py " \
    #        f"{pdf_folder_name} {parid}\n"
    line = f"module load miniconda; conda activate ragflow; python ragflow_chunking.py " \
           f"{pdf_folder_name} {parid}\n"
    file1.writelines(line)

# Run parsing for 1026
import pickle
file_name = f"run_ragflow_api_calls_1026.txt"
pdf_folder_name = 'raw_metadata_10262024_pdfs'
print('dsq --job-file ' + file_name + ' --cpus-per-task=4 --mem=180G --time=1-00:00:00 --output=JOBLOG_ragflow_parsing_1026/dsq-jobfile-%A_%a-%N.out -p scavenge')
file1 = open(file_name, "w")

# create parids
metadata_folder = os.path.join(BASE_FOLDER, 'firefox_download')
temp_df = []
for file in os.listdir(metadata_folder):
    res = pickle.load(open(os.path.join(metadata_folder, file), 'rb'))
    for x in res:
        temp_df.append([file.split('.')[0], x['save_pdf_name']])
import numpy as np
num_runs_in_each_partition = 500
index = range(len(temp_df))
partitions = np.split(np.array(index).astype(int), np.arange(num_runs_in_each_partition,
                                                             len(index), num_runs_in_each_partition).astype(
    int))

partitions_save_folder = os.path.join(BASE_FOLDER, 'raw_metadata_10262024_pdfs_partition')
for parid in range(len(partitions)):
    temp_run = temp_df[partitions[parid][0]:partitions[parid][-1]]
    pickle.dump(temp_run, open(os.path.join(partitions_save_folder, f'parid_{parid}'), 'wb'))



for parid in range(4640):
    # line = f"export HF_ENDPOINT=https://hf-mirror.com; export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK; " \
    #        f"module load miniconda; conda activate ragflow; python ragflow_chunking.py " \
    #        f"{pdf_folder_name} {parid}\n"
    line = f"module load miniconda; conda activate ragflow; python ragflow_chunking.py " \
           f"{pdf_folder_name} {parid}\n"
    file1.writelines(line)