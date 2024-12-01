import os
import argparse
import time
import pandas as pd
import parmap
import multiprocessing as mp
from functools import partial
from src_gpt import *

def main(args):
	st = time.time()
	cwd = os.getcwd()
	idx = args.idx

	path = os.path.join(
		cwd,
		str(idx),
		'google_organic_'+str(idx)+'.csv'
	)
	df_organic = pd.read_csv(path)

	kg_path = os.path.join(
		cwd,
		str(idx),
		'google_knowledge_graph_' + str(idx) + '.csv'
	)
	df_kg = pd.read_csv(kg_path) # may need option: lineterminator='\n'

	places_path = os.path.join(
		cwd,
		str(idx),
		'google_places_' + str(idx) + '.csv'
	)
	df_places = pd.read_csv(places_path)

	answer_box_path = os.path.join(
		cwd,
		str(idx),
		'google_answer_box_' + str(idx) + '.csv'
	)
	df_answer_box = pd.read_csv(answer_box_path)

	query_list = list(df_organic['query'])
	query_unique_list = list(set(query_list))
	print("Number of unique queries:", len(query_unique_list))

	fn_ = partial(
		extract_search_result,
		df_organic=df_organic
	)

	result_list = parmap.map(
		fn_,
		query_unique_list,
		pm_pbar=True,
		pm_processes=mp.cpu_count(),
	)

	new_query_list = [result[0] for result in result_list]
	new_result_list = [result[1] for result in result_list]
	kg_title_list = []
	places_title_list = []
	answer_box_title_list = []

	for query in query_unique_list:
		query = str(query)
		kg_title = extract_knowledge_graph(
			df_kg=df_kg,
			query=query,
		)
		places_title = extract_places(
			df_places=df_places,
			query=query,
		)
		answer_box_title = extract_answer_box(
			df_answer_box=df_answer_box,
			query=query,
		)
		kg_title_list.append(kg_title)
		places_title_list.append(places_title)
		answer_box_title_list.append(answer_box_title)

	df_final = pd.DataFrame({})
	df_final['query'] = query_unique_list # Retain 'df_final['query']' as it will be used as the key to merge this dataset with others later.
	df_final['new_query'] = new_query_list
	df_final['search_result'] = new_result_list
	df_final['kg_title'] = kg_title_list
	df_final['places_title'] = places_title_list
	df_final['answer_box_title'] = answer_box_title_list

	output_csv = os.path.join(
		cwd,
		str(idx),
		'google_organic_to_gpt_'+str(idx)+'.csv'
	)
	df_final.to_csv(output_csv, index=False)

	et = time.time()
	print ("Time for execution:", round(et-st, 2), "(s)")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()

	main(args)
