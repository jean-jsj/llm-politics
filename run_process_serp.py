import os
import time
import argparse
from src import src_google_serp

def main(args):
	cwd = os.getcwd()
	idx = args.idx
	st = time.time()
	print("Begin index: ", idx)
	dir_ = os.path.join(
		cwd,
		str(idx),
	)
	content_list = src_google_serp.get_content_list(
		idx=idx,
	)
	# Summary
	src_google_serp.google_summary(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Knowledge Graph
	src_google_serp.google_knowledge_graph(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Places
	src_google_serp.google_places(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# Organic
	src_google_serp.google_organic(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# RelatedSearches
	src_google_serp.google_related_searches(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# PeopleAlsoAsk
	src_google_serp.google_also_ask(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# AnswerBox
	src_google_serp.google_answer_box(
		dir_=dir_,
		idx=idx,
		content_list=content_list
	)
	# TopStories
	src_google_serp.google_top_stories(
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
