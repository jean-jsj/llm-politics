import os
import time
import argparse
import pandas as pd
import xml.etree.ElementTree as ET
from openai import OpenAI
from src_gpt import *


def main(args):
	'''
	To handle occasional API call failures, the data must be processed in batches of num_unit. 
    A separate for-loop is used for each batch, where the 'answer_list' variable is defined and saved to a CSV file.  
    Since the size of the input data ({type}_total lists) and the size of the data processed within each for-loop (answer_list) differ, 
	we define 'sub_index_list' to ensure consistent indexing across all batches.
	'''
	cwd = os.getcwd()
	idx = args.idx
	OPENAI_API_KEY = get_api_key_gpt(idx=idx)
	client = OpenAI(api_key=OPENAI_API_KEY)

	path = os.path.join(
		cwd,
		str(idx),
		'google_organic_to_gpt_'+str(idx)+'.csv'
	)
	df = pd.read_csv(path)
	df = df.dropna(subset=['search_result'])

	query_total = list(df['query'])
	new_query_total = list(df['new_query'])
	search_result_total = list(df['search_result'])
	kg_title_total = list(df['kg_title'])
	places_title_total = list(df['places_title'])
	answer_box_title_total = list(df['answer_box_title'])
	
	# Processed data in batches of num_unit
	num_total = len(df)
	num_unit = args.num_unit
	num_files = num_total // num_unit
	if num_total % num_unit > 0:
		num_files += 1

	for i in range(num_files):
		st = time.time()
		start = i*num_unit
		end = (i+1)*num_unit
		if i == (num_files-1):
			end = num_total

		sub_index_list = list(range(start, end))
		# Pre-fill answer_list with 'ERROR' in case of GPT API call failure. If the API call is successful, replace 'ERROR' with the GPT response.
		answer_list = ['ERROR' for _ in range(len(sub_index_list))]
		answer_path = os.path.join(
			cwd,
			str(idx),
			'gpt_answer_'+str(idx)+'_'+str(start)+'_'+str(end)+'.csv'
		)
		## If, after running the entire code in this Python file, the 'answer_list' variable in the saved data is marked 'ERROR', it indicates an API call failure.
	    ## Uncomment the following lines and re-run the entire file to handle those cases.
		#if os.path.exists(answer_path):
		#	df_answer = pd.read_csv(answer_path)
		#	df_answer['sub_index'] = sub_index_list
		#	answer_list = list(df_answer['answer'])
		#	condition = (df_answer['answer'] == 'ERROR')
		#	df_answer = df_answer[condition]
		#	sub_index_list = list(df_answer['sub_index'])

		for sub_index in sub_index_list:
			j = sub_index - start
			if (str(kg_title_total[sub_index]) == 'nan') and (str(places_title_total[sub_index]) == 'nan') and (str(answer_box_title_total[sub_index]) == 'nan'):
				answers = get_gpt_answers(
					client=client,
					query=new_query_total[sub_index],
					search_results=search_result_total[sub_index],
					max_tokens=args.max_tokens,
				)
				answer_list[j] = answers.replace('\n', '')
			else:
				answer_list[j] = 'ANSWER_EXISTS'

		df = pd.DataFrame({})
		df['query'] = query_total[start:end] # Retain 'df['query']' as it will be used as the key to merge this dataset with others later.
		df['new_query'] = new_query_total[start:end]
		df['answer'] = answer_list
		df.to_csv(answer_path, index=False)
		
		et = time.time()
		print ("Start:", start, "\t End:", end, "\t Num runs:", len(sub_index_list), "\t Time:", round(et-st, 2), "(s)")


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
