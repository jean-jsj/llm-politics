import os
import time
import argparse
import pandas as pd
from src import src_standardize


def get_df(
		cwd,
		idx,
		type_,
	):
	path = os.path.join(
		cwd,
		str(idx),
		'google_'+type_+'_'+str(idx)+'.csv'
	)
	df = pd.read_csv(path, engine='python')
	return df


def main(args):
	cwd = os.getcwd()
	idx = args.idx
	st = time.time()
	print("Begin index: ", idx)
	
	df_summary = get_df(
		cwd=cwd,
		idx=idx,
		type_='summary'
	)
	df_organic = get_df(
		cwd=cwd,
		idx=idx,
		type_='organic'
	)
	df_knowledgeGraph = get_df(
		cwd=cwd,
		idx=idx,
		type_='knowledge_graph'
	)
	df_answerBox = get_df(
		cwd=cwd,
		idx=idx,
		type_='answer_box'
	)
	df_places = get_df(
		cwd=cwd,
		idx=idx,
		type_='places'
	)
	output_path = os.path.join(
		cwd,
		str(idx),
		'standardize_'+str(idx)+'.csv'
	)
	
	src_standardize.run_standardization(
		df_summary=df_summary,
		df_organic=df_organic,
		df_knowledgeGraph=df_knowledgeGraph,
		df_answerBox=df_answerBox,
		df_places=df_places,
		output_path=output_path,
	)
	
	et = time.time()
	print ("Time spent:", round(et-st, 2), "(s)")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()

	main(args)
