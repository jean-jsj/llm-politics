import re
import os
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY_DICT = os.environ.get('OPENAI_API_KEY_DICT')

SYSTEM_PROMPT = """
You are a senior nlp assistant that identifies nouns in the given query, using the given context(delimited with XML tags) as reference. Let's try this step by step.

Step 1: decompose the given query into individual words and insert spaces between them (for example, change "donaldtrumpage" to "donald trump age"). 

Step 2: identify a proper noun in the query. If a proper noun consists of multiple words, treat the entire sequence of words as a single noun.
A proper noun is a specific name used to identify distinct entities such as creative works (such as songs, novels, television programs, or films), individuals, organizations or institutions, locations, cities, buildings, brands, companies, restaurants, healthcare facilities, events, sport tems, and more.

Step 3: identify a common noun in the query.

Return your response in the following format.
Decomposed query: ;;
Proper noun: ;;
Common noun: ;;
"""

PROMPT = """
<start_of_context>{search_results}<end_of_context>

<start_of_query>{query}<end_of_query>
"""


def refine_query(query, first_title):
	new_query = query
	if ' - Wikipedia' in first_title: 
		new_query = first_title.split(' - ')[0]
	return new_query


def extract_search_result(query, df):
	'''
	1. Process raw Google search results into a format suitable for GPT input. 
    2. Extract the top 5 results for each query and wraps them in <title> and <snippet> tags
    '''
	condition = (df['query'] == query)
	df_ = df[condition][:5]
	title_list = list(df_['title'])
	snippet_list = list(df_['snippet'])

	query = refine_query(
		query=query,
		first_title=title_list[0]
	)

	result = ''
	for i in range(len(df_)):
		result += '<title>'
		result += str(title_list[i])
		result += '</title>'
		result += '<snippet>'
		result += str(snippet_list[i])
		result += '</snippet>'
	return query, result


def extract_knowledge_graph(query, df_kg):
	'''
	1. Retrieve search engine results for each query and check whether they include Google Knowledge Graph data. 
	2. If found, extract the 'title' field from the data
	'''
	condition = (df_kg['query'] == query)
	df_ = df_kg[condition]
	kg_title = ''
	if len(df_) > 0:
		title_ = str(list(df_['title'])[0])
		if title_ != 'nan':
			kg_title = title_
	return kg_title


def extract_places(query, df_places):
	'''
	1. Retrieve search engine results for each query and check whether they include Google Places data. 
	2. If found, extract the 'title' field from the data
	'''
	condition = (df_places['query'] == query)
	df_ = df_places[condition]
	places_title = ''
	if len(df_) > 0:
		title_ = str(list(df_['title'])[0])
		if title_ != 'nan':
			places_title = title_
	return places_title


def extract_answer_box(query, df_answer_box):
	'''
	1. Retrieve search engine results for each query and check whether they include Google Answer Box data. 
	2. If found, extract the 'title' field from the data
	'''
	condition = (df_answer_box['query'] == query)
	df_ = df_answer_box[condition]
	answer_box_title = ''
	if len(df_) > 0:
		title_ = str(list(df_['title'])[0])
		if title_ != 'nan':
			answer_box_title = title_
	return answer_box_title


def get_api_key_gpt(idx):
	'''
	Retrieve the API key assigned to each CPU from OPENAI_API_KEY_DICT
	'''
	num_keys = len(OPENAI_API_KEY_DICT)
	key = (idx-1) % 10
	api_key = OPENAI_API_KEY_DICT[key][0]
	return api_key


def get_completion(
		client,
		messages,
		model,
		max_tokens,
		temperature,
		top_p,
	):
	params = {
		'messages': messages,
		'model': model,
		'max_tokens': max_tokens,
		'temperature': temperature,
		'top_p': top_p,
	}
	completion = client.chat.completions.create(**params)
	return completion


def get_gpt_answers(
		client,
		query,
		search_results,
		model='gpt-3.5-turbo-0125',
		max_tokens=100,
		temperature=0,
		top_p=1,
	):
	USER_PROMPT = PROMPT.format(
		search_results=search_results,
		query=query,
	)
	messages = [
		{"role": "system", "content": SYSTEM_PROMPT},
		{"role": "user", "content": USER_PROMPT},
	]

	answers = ''
	try:
		response = get_completion(
			client=client,
			messages=messages,
			model=model,
			max_tokens=max_tokens,
			temperature=temperature,
			top_p=top_p,
		)
		answers = response.choices[0].message.content
	except:
		answers = 'ERROR'
	return answers


def parse_gpt_answers(df):
    answer_list = list(df['answer'])
    de_query_list = []
    proper_nouns_list = []
    common_nouns_list = []
    for answer in answer_list:
        de_query = ''
        proper_nouns = ''
        common_nouns = ''
        try:
            de_query = answer.split('Proper noun: ')[0].replace('Decomposed query: ', '')
            proper_nouns = answer.split('Proper noun: ')[1].split('Common noun:')[0].replace('Proper noun: ','').replace('없음', '')
            common_nouns = answer.split('Common noun: ')[1].replace('없음', '')
            if ';;' in de_query:
                de_query = de_query.replace(';;', '')
                if ';;' in proper_nouns:
                    proper_nouns = proper_nouns.replace(';;', '')
                    if ';' in common_nouns:
                        common_nouns = common_nouns.split(';')[0]

            pattern = r'\([^)]*\)'
            if '(' not in de_query and '(' in proper_nouns and '(주)' not in proper_nouns:
                proper_nouns = re.sub(pattern=pattern, repl='', string=proper_nouns)

            if len(proper_nouns.split(',')) > 2:
                proper_nouns = proper_nouns[0]

        except:
            de_query = ''
            proper_nouns = ''
            common_nouns = ''

        de_query_list.append(de_query.strip())
        proper_nouns_list.append(proper_nouns.strip())
        common_nouns_list.append(common_nouns.strip())

    df['DecomposedQuery'] = de_query_list
    df['ProperNouns'] = proper_nouns_list
    df['CommonNouns'] = common_nouns_list
    return df