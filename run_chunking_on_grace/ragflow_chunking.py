import logging
import os
import sys
import timeit
import pickle

if 'sk2248' in os.getcwd():
    sys.path.extend(r'/gpfs/gibbs/project/kelly/sk2248/CorePython/experiments/')
    sys.path.append(r'/gpfs/gibbs/project/kelly/sk2248/CorePython/experiments/')
    sys.path.extend(r'/gpfs/gibbs/project/kelly/sk2248/ragflow/')
    sys.path.append(r'/gpfs/gibbs/project/kelly/sk2248/ragflow/')
    BASE_FOLDER = r'/vast/palmer/scratch/kelly/sk2248/analyst_reports/'
else:
    BASE_FOLDER = r'E:\analyst_reports'

from rag.app.naive import chunk


def dummy(prog=None, msg=""):
    # print(msg)
    pass


if __name__ == '__main__':
    pdf_folder_name = str(sys.argv[1])
    parid = int(sys.argv[2])
    partition_run_folder = os.path.join(BASE_FOLDER, pdf_folder_name + '_partition')

    partition_run = pickle.load(open(os.path.join(partition_run_folder, f'parid_{parid}'), 'rb'))

    ragflow_parsed_chunk_folder_text = os.path.join(BASE_FOLDER, 'ragflow_chunks_text')
    os.makedirs(ragflow_parsed_chunk_folder_text, exist_ok=True)
    ragflow_parsed_chunk_folder_text_table = os.path.join(BASE_FOLDER, 'ragflow_chunks_text_table')
    os.makedirs(ragflow_parsed_chunk_folder_text_table, exist_ok=True)
    ragflow_parsed_chunk_folder_text = os.path.join(ragflow_parsed_chunk_folder_text, pdf_folder_name)
    os.makedirs(ragflow_parsed_chunk_folder_text, exist_ok=True)
    ragflow_parsed_chunk_folder_text_table = os.path.join(ragflow_parsed_chunk_folder_text_table, pdf_folder_name)
    os.makedirs(ragflow_parsed_chunk_folder_text_table, exist_ok=True)

    for i, (folder_name, pdf_path) in enumerate(partition_run):


        ragflow_parsed_chunk_folder_text_quarter = os.path.join(ragflow_parsed_chunk_folder_text, folder_name)
        os.makedirs(ragflow_parsed_chunk_folder_text_quarter, exist_ok=True)
        ragflow_parsed_chunk_folder_text_table_quarter = os.path.join(ragflow_parsed_chunk_folder_text_table, folder_name)
        os.makedirs(ragflow_parsed_chunk_folder_text_table_quarter, exist_ok=True)
        if os.path.exists(os.path.join(ragflow_parsed_chunk_folder_text_table_quarter, pdf_path.split('.pdf')[0])):
            continue

        start = timeit.default_timer()
        try:
            res = chunk(filename=os.path.join(BASE_FOLDER, pdf_folder_name, folder_name, pdf_path),
                        callback=dummy, lang='English')
        except AssertionError as e:
            logging.error(str(e))
            logging.info(f'Error: Filename {folder_name}-{pdf_path}. Total time {end - start}.')
            continue
        res_content_with_table = [x['content_with_weight'] for x in res]
        res_content = [x['content_with_weight'] for x in res if '<table>' not in x['content_with_weight']]
        # save
        pickle.dump(res_content_with_table, open(os.path.join(ragflow_parsed_chunk_folder_text_table_quarter, pdf_path.split('.pdf')[0]), 'wb'))
        pickle.dump(res_content, open(os.path.join(ragflow_parsed_chunk_folder_text_quarter, pdf_path.split('.pdf')[0]), 'wb'))

        end = timeit.default_timer()
        if i % 10 == 0:
            logging.info(f'Finished {i} out of {len(partition_run)}. Filename {folder_name}-{pdf_path}. Total time {end - start}. Total num of chunks: {len(res_content_with_table)}. Total number of nontable_chunks: {len(res_content)}.')
