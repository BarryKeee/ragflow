import os
import sys
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
print('dsq --job-file ' + file_name + ' --cpus-per-task=4 --mem=16G --time=1-00:00:00 --output=JOBLOG_ragflow_parsing/dsq-jobfile-%A_%a-%N.out -p scavenge')
file1 = open(file_name, "w")
for parid_file in os.listdir(partition_run_folder):
    parid = parid_file.split('_')[1]
    # line = f"export HF_ENDPOINT=https://hf-mirror.com; export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK; " \
    #        f"module load miniconda; conda activate ragflow; python ragflow_chunking.py " \
    #        f"{pdf_folder_name} {parid}\n"
    line = f"module load miniconda; conda activate ragflow; python ragflow_chunking.py " \
           f"{pdf_folder_name} {parid}\n"
    file1.writelines(line)