import os
import time
import glob
import ast
import pandas as pd
import http.client
import json


def api_google_serp(query, SERP_API_KEY):
	conn = http.client.HTTPSConnection("google.serper.dev")
	payload = json.dumps({
		"q": query
	})
	headers = {
		'X-API-KEY': SERP_API_KEY,
		'Content-Type': 'application/json'
	}
	conn.request(
		"POST",
		"/search",
		payload,
		headers,
	)
	response = conn.getresponse()
	data = response.read()
	result = data.decode("utf-8")
	return result

def run_api_google_serp(cwd,idx,num_unit,SERP_API_KEY):
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
	num_files = num_total // num_unit
	if num_total % num_unit > 0:
		num_files += 1

	for i in range(num_files):
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
			result =api_google_serp(query=entity, SERP_API_KEY=SERP_API_KEY)
			g.write(result+'\n')
			g.close()

def get_content_list(idx):
	content_list = []

	path_list = glob.glob(f'./{idx}/search_{idx}_*.txt')
	print("IDX: ", idx, " has how many files: ",len(path_list))
	for path in path_list:
		f = open(path,'r')
		lines = f.readlines()
		f.close()
		for line in lines:
			try:
				# parse a string containing a python literal into a python dictionary
				content = ast.literal_eval(line)
				if content is not None:
					key_list = list(content.keys())
					if key_list is not None and 'searchParameters' in key_list:
						content_list.append(content)
			except:
				pass
	return content_list


def get_summary(content):
	key_list = list(content.keys())
	query = ''
	knowledge_graph = 0
	organic = 0
	related_searches = 0
	people_also_ask = 0
	answer_box = 0
	top_stories = 0
	images = 0
	places = 0

	result = []
	if key_list is not None:
		query = content['searchParameters']['q']
		if 'knowledgeGraph' in key_list:
			knowledge_graph = 1
		if 'organic' in key_list:
			organic = 1
		if 'relatedSearches' in key_list:
			related_searches = 1
		if 'peopleAlsoAsk' in key_list:
			people_also_ask = 1
		if 'answerBox' in key_list:
			answer_box = 1
		if 'topStories' in key_list:
			top_stories = 1
		if 'images' in key_list:
			images = 1
		if 'places' in key_list:
			places = 1
		result = [
			query,
			knowledge_graph,
			organic,
			related_searches,
			people_also_ask,
			answer_box,
			top_stories,
			images,
			places,
		]
		return result
	else:
		return -1


def google_summary(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'knowledgeGraph',
		'organic',
		'relatedSearches',
		'peopleAlsoAsk',
		'answerBox',
		'topStories',
		'images',
		'places',
	]
	summary_list = []
	for content in content_list:
		summary = get_summary(content)
		if summary != -1:
			summary_list.append(summary)
	df = pd.DataFrame(summary_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_summary_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for initial summary:", round(et-st, 2), "(s)")


def get_knowledge_graph(content):
	key_list = list(content.keys())

	query = ''
	title = ''
	type_ = ''
	description = ''
	attributes = ''
	if key_list is not None:
		query = content['searchParameters']['q']
		if 'knowledgeGraph' in key_list:
			kg_list = content['knowledgeGraph']
			kg_key_list = list(kg_list.keys())
			if 'title' in kg_key_list:
				title = kg_list['title'].replace('\n', '')
			if 'type' in kg_key_list:
				type_ = kg_list['type'].replace('\n', '')
			if 'description' in kg_key_list:
				description = kg_list['description'].replace('\n', '')
			if 'attributes' in kg_key_list:
				attributes_dict = list(kg_list['attributes'].keys())
				attributes = attributes_dict[0]
		result = [
			query,
			title,
			type_,
			description,
			attributes,
		]
		return result
	else:
		return -1


def google_knowledge_graph(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'title',
		'type',
		'description',
		'attributes',
	]
	result_list = []
	for content in content_list:
		result = get_knowledge_graph(content)
		if result != -1:
			result_list.append(result)
	df = pd.DataFrame(result_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_knowledge_graph_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for knowlege graph:", round(et-st, 2), "(s)")


def get_organic(content):
	key_list = list(content.keys())

	if key_list is not None:
		query = content['searchParameters']['q']
		result_list = []
		if 'organic' in key_list:
			organic_list = content['organic']
			for organic in organic_list:
				title = ''
				link = ''
				date = ''
				snippet = '' 
				org_key_list = list(organic.keys())
				if 'title' in org_key_list:
					title = organic['title'].replace('\n', '')
				if 'date' in org_key_list:
					date = organic['date'].replace('\n', '')
				if 'link' in org_key_list:
					link = organic['link'].replace('\n', '')
				if 'snippet' in org_key_list:
					snippet = organic['snippet'].replace('\n', '')
				result = [
					query,
					title,
					date,
					link,
					snippet,
				]
				result_list.append(result)
		else:
			result = [
				query,
				'',
				'',
				'',
				'',
			]
			result_list.append(result)
		return result_list
	else:
		return -1


def google_organic(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'title',
		'date',
		'link',
		'snippet',
	]
	result_list = []
	for content in content_list:
		result = get_organic(content)
		if result != -1:
			result_list.extend(result)
	df = pd.DataFrame(result_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_organic_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for organic:", round(et-st, 2), "(s)")


def get_related_searches(content):
	key_list = list(content.keys())
	query = ''
	related = ''
	if key_list is not None:
		query = content['searchParameters']['q']
		if 'relatedSearches' in key_list:
			related_list = content['relatedSearches']
			query_list = [val['query'] for val in related_list]
			if len(related_list) > 0:
				related = ':'.join(query_list)
		result = [
			query,
			related,
		]
		return result
	else:
		return -1


def google_related_searches(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'related_searches',
	]
	result_list = []
	for content in content_list:
		result = get_related_searches(content)
		if result != -1:
			result_list.append(result)
	df = pd.DataFrame(result_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_related_searches_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for related searches:", round(et-st, 2), "(s)")


def get_also_ask(content):
	key_list = list(content.keys())

	if key_list is not None:
		query = content['searchParameters']['q']
		result_list = []
		if 'peopleAlsoAsk' in key_list:
			ask_list = content['peopleAlsoAsk']
			for ask_ in ask_list:
				question = ''
				title = ''
				snippet ='' 
				link = ''
				ask_key_list = list(ask_.keys())
				if 'question' in ask_key_list:
					question = ask_['question'].replace('\n', '')
				if 'title' in ask_key_list:
					title = ask_['title'].replace('\n', '')
				if 'snippet' in ask_key_list:
					snippet = ask_['snippet'].replace('\n', '')
				if 'link' in ask_key_list:
					link = ask_['link'].replace('\n', '')
				result = [
					query,
					question,
					title,
					snippet,
					link,
				]
				result_list.append(result)
		else:
			result = [
				query,
				'',
				'',
				'',
				'',
			]
			result_list.append(result)
		return result_list
	else:
		return -1


def google_also_ask(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'question',
		'title',
		'link',
		'snippet',
	]
	result_list = []
	for content in content_list:
		result = get_also_ask(content)
		if result != -1:
			result_list.extend(result)
	df = pd.DataFrame(result_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_also_ask_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for also ask:", round(et-st, 2), "(s)")


def get_answer_box(content):
	key_list = list(content.keys())

	query = ''
	title = ''
	snippet = ''
	link = ''
	if key_list is not None:
		query = content['searchParameters']['q']
		if 'answerBox' in key_list:
			answer_list = content['answerBox']
			answer_key_list = list(answer_list.keys())
			if 'title' in answer_key_list:
				title = answer_list['title'].replace('\n', '')
			if 'snippet' in answer_key_list:
				snippet = answer_list['snippet'].replace('\n', '')
			if 'link' in answer_key_list:
				link = answer_list['link'].replace('\n', '')
		result = [
			query,
			title,
			snippet,
			link,
		]
		return result
	else:
		return -1


def google_answer_box(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'title',
		'snippet',
		'link',
	]
	result_list = []
	for content in content_list:
		result = get_answer_box(content)
		if result != -1:
			result_list.append(result)
	df = pd.DataFrame(result_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_answer_box_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for answer box:", round(et-st, 2), "(s)")


def get_top_stories(content):
	key_list = list(content.keys())

	if key_list is not None:
		query = content['searchParameters']['q']
		result_list = []
		if 'topStories' in key_list:
			story_list = content['topStories']
			for story in story_list:
				title = ''
				source = ''
				date = ''
				link = ''
				story_key_list = list(story.keys())
				if 'title' in story_key_list:
					title = story['title'].replace('\n', '')
				if 'source' in story_key_list:
					source = story['source'].replace('\n', '')
				if 'date' in story_key_list:
					date = story['date'].replace('\n', '')
				if 'link' in story_key_list:
					link = story['link'].replace('\n', '')
				result = [
					query,
					title,
					source,
					date,
					link,
				]
				result_list.append(result)
		else:
			result = [
				query,
				'',
				'',
				'',
				'',
			]
			result_list.append(result)
		return result_list
	else:
		return -1


def google_top_stories(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'title',
		'source',
		'date',
		'link',
	]
	result_list = []
	for content in content_list:
		result = get_top_stories(content)
		if result != -1:
			result_list.extend(result)
	df = pd.DataFrame(result_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_top_stories_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for top stories:", round(et-st, 2), "(s)")


def get_places(content):
	key_list = list(content.keys())

	if key_list is not None:
		query = content['searchParameters']['q']
		result_list = []
		if 'places' in key_list:
			place_list = content['places']
			place = place_list[0]
			title = ''
			address = ''
			place_key_list = list(place.keys())
			if 'title' in place_key_list:
				title = place['title'].replace('\n', '')
			if 'address' in place_key_list:
				address = place['address'].replace('\n', '')
			result = [
				query,
				title,
				address,
			]
		else:
			result = [
				query,
				'',
				'',
			]
		return result
	else:
		return -1


def google_places(
		dir_,
		idx,
		content_list,
	):
	st = time.time()
	columns = [
		'query',
		'title',
		'address',
	]
	result_list = []
	for content in content_list:
		result = get_places(content)
		if result != -1:
			result_list.append(result)
	
	df = pd.DataFrame(result_list, columns=columns)
	csv_path = os.path.join(
		dir_,
		'google_places_' + str(idx) + '.csv',
	)
	df.to_csv(csv_path, index=False)
	et = time.time()
	print ("Time for place:", round(et-st, 2), "(s)")

