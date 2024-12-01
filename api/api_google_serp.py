import os
import argparse
import pandas as pd
from dotenv import load_dotenv
import time
from src_google_serp import *

load_dotenv()
SERP_API_KEY = os.environ.get('SERP_API_KEY')

def main(args):
	cwd = os.getcwd()
	idx = args.idx
	st = time.time()

	input_path = os.path.join(
		cwd,
		str(idx),
		'raw_'+str(idx)+'.txt'
	)
	f = open(input_path, 'r')
	lines = f.readlines()
	f.close()
	entity_list = [l.strip() for l in lines]
	
    # Processed data in batches of num_unit
	num_total = len(entity_list)
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
			
		entities = entity_list[start:end]
		output_path = os.path.join(
			cwd,
    		str(idx),
			'search_'+str(idx)+'_'+str(start)+'_'+str(end)+'.txt'
		)
		g = open(output_path,'w')
		for entity in entities:
			result = google_search_with_http(query=entity, SERP_API_KEY=SERP_API_KEY)
			g.write(result+'\n')
			g.close()
		et = time.time()
		print("Finished scraping #", str(start),",\t Time for scraping: ",round(et-st,2), "(s)")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()
	main(args)
