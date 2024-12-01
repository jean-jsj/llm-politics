import os
import glob
import pandas as pd
import argparse
from src_gpt import *

def main(args):
	cwd = os.getcwd()
	idx = args.idx

	df_list = []
	path_list = glob.glob(f'./{idx}/gpt_answer_{idx}_*.csv')
	for path in path_list:
		df = pd.read_csv(path)
		df_list.append(df)
	df = pd.concat(df_list)

	## Uncomment the following lines to count how many entries in the dataset have 'answer' set to 'ERROR' as a result of API call failures:
	#condition = (df['answer'] == 'ERROR')
	#df_ = df[condition]
	#num_error = len(df_)
	#print ("IDX:", idx, "\tNumber of error terminated queries:", num_error)

	df_edited = parse_gpt_answers(df=df)
	output_path = os.path.join(
		cwd,
		str(idx),
		'gpt_answer_edit_'+str(idx)+'.csv'
	)
	df_edited.to_csv(output_path, index=False)

	

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()

	main(args)