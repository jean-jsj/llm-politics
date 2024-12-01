import os
import time
import argparse
from src_google_serp import *

def main(args):
	cwd = os.getcwd()
	idx = args.idx
	st = time.time()
	print("Begin index: ", idx)
	dir_ = os.path.join(
		cwd,
		str(idx),
	)
	content_list = get_content_list(
		idx=idx,
	)
	# Summary
	google_summary(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Knowledge Graph
	google_knowledge_graph(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Places
	google_places(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Organic
	google_organic(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# RelatedSearches
	google_related_searches(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# PeopleAlsoAsk
	google_also_ask(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# AnswerBox
	google_answer_box(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# TopStories
	google_top_stories(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	et = time.time()
	print ("Time spent:", round(et-st, 2), "(s)")


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()

	main(args)
