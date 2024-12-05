import os
import time
import argparse
from src import src_process_serp
from src import src_naver_news
from dotenv import load_dotenv

load_dotenv()
SERP_API_KEY = os.environ.get('SERP_API_KEY')
NAVER_DICT = os.getenv("NAVER_DICT")

def main(args):
	cwd = os.getcwd()
	idx = args.idx
	num_unit = args.num_unit

	st = time.time()
	print("Begin idx: ", idx)
	dir_ = os.path.join(
		cwd,
		str(idx),
	)

	# Google SERP API
	st = time.time()
	src_process_serp.run_api_google_serp(cwd,idx,num_unit,SERP_API_KEY)
	et = time.time()
	print ("Run Google SERP API: ", round(et-st, 2), "(s)")

	# Process raw data
	st = time.time()
	content_list = src_process_serp.get_content_list(
		idx=idx,
	)
	src_process_serp.google_summary(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Knowledge Graph
	src_process_serp.google_knowledge_graph(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Places
	src_process_serp.google_places(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Organic
	src_process_serp.google_organic(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# RelatedSearches
	src_process_serp.google_related_searches(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# PeopleAlsoAsk
	src_process_serp.google_also_ask(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# AnswerBox
	src_process_serp.google_answer_box(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# TopStories
	src_process_serp.google_top_stories(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	et = time.time()
	print ("Process Google SERP API: ", round(et-st, 2), "(s)")

	# NAVER NEWS API
	st = time.time()
	src_naver_news.run_api_naver_news(cwd,idx,NAVER_DICT)
	et = time.time()
	print ("NAVER NEWS API: ", round(et-st, 2), "(s)")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()

	main(args)
