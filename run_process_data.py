import os
import time
import argparse
import pandas as pd
import xml.etree.ElementTree as ET
from openai import OpenAI
from src import src_gpt
from src import src_standardize
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY_DICT = os.environ.get('OPENAI_API_KEY_DICT')

def get_api_key_gpt(idx):
	'''
	Retrieve the API key assigned to each CPU from OPENAI_API_KEY_DICT
	'''
	key = (idx-1) % 10
	api_key = OPENAI_API_KEY_DICT[key][0]
	return api_key

def main(args):
	cwd = os.getcwd()
	idx = args.idx
	num_unit = args.num_unit
	max_tokens=args.max_tokens,

	OPENAI_API_KEY = get_api_key_gpt(idx=idx)
	client = OpenAI(api_key=OPENAI_API_KEY)

	st = time.time()
	df = src_gpt.prepare_gpt_input(cwd,idx)
	et = time.time()
	print ("Prepare OpenAI API input: ", "\t Time:", round(et-st, 2), "(s)")

	st = time.time()
	src_gpt.run_api_gpt(cwd,idx,num_unit,max_tokens,client,df)		
	et = time.time()
	print ("Run OpenAI API: ", "\t Time:", round(et-st, 2), "(s)")

	st = time.time()
	src_gpt.run_parse_gpt_answers(cwd,idx)
	et = time.time()
	print ("Parse OpenAI API response: ", "\t Time:", round(et-st, 2), "(s)")

	df_summary = src_standardize.get_df(
		cwd=cwd,
		idx=idx,
		type_='summary'
	)
	df_organic = src_standardize.get_df(
		cwd=cwd,
		idx=idx,
		type_='organic'
	)
	df_knowledgeGraph = src_standardize.get_df(
		cwd=cwd,
		idx=idx,
		type_='knowledge_graph'
	)
	df_answerBox = src_standardize.get_df(
		cwd=cwd,
		idx=idx,
		type_='answer_box'
	)
	df_places = src_standardize.get_df(
		cwd=cwd,
		idx=idx,
		type_='places'
	)
	output_path = os.path.join(
		cwd,
		str(idx),
		'standardize_'+str(idx)+'.csv'
	)
	
	st = time.time()
	src_standardize.run_standardization(
		df_summary=df_summary,
		df_organic=df_organic,
		df_knowledgeGraph=df_knowledgeGraph,
		df_answerBox=df_answerBox,
		df_places=df_places,
		output_path=output_path,
	)
	et = time.time()
	print ("Run Standardization: ", "\t Time:", round(et-st, 2), "(s)")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	parser.add_argument('--num_unit', type=int, default=1000,
						help='number of unit queries')
	parser.add_argument('--max_tokens', type=int, default=1000,
						help='max tokens for outputs')
	parser.add_argument('--num_files', type=int, default=50,
						help='Number of files')
	args = parser.parse_args()

	main(args)
