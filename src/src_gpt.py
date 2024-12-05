import re
import os
import pandas as pd
import glob
import parmap
import multiprocessing as mp
from functools import partial

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


def prepare_gpt_input(cwd,idx):
	path = os.path.join(
		cwd,
		str(idx),
		'google_organic_'+str(idx)+'.csv'
	)
	df_organic = pd.read_csv(path)
	query_list = list(df_organic['query'])
	query_unique_list = list(set(query_list))

	fn_ = partial(
		extract_search_result,
		df_organic=df_organic
	)

	result_list = parmap.map(
		fn_,
		query_unique_list,
		pm_pbar=True,
		pm_processes=mp.cpu_count(),
	)

	new_query_list = [result[0] for result in result_list]
	new_result_list = [result[1] for result in result_list]

	df_final = pd.DataFrame({})
	df_final['query'] = query_unique_list # Retain 'df_final['query']' as it will be used as the key to merge this dataset with others later.
	df_final['new_query'] = new_query_list
	df_final['search_result'] = new_result_list
	df_final = df_final.dropna(subset=['search_result'])
	return df_final


def run_api_gpt(cwd,idx,num_unit,max_tokens,client,df):
	'''
	To handle occasional API call failures, the data must be processed in batches of num_unit. 
    A separate for-loop is used for each batch, where the 'answer_list' variable is defined and saved to a CSV file.  
    Since the size of the input data ({type}_total lists) and the size of the data processed within each for-loop (answer_list) differ, 
	we define 'sub_index_list' to ensure consistent indexing across all batches.
	'''

	query_total = list(df['query'])
	new_query_total = list(df['new_query'])
	search_result_total = list(df['search_result'])
	
	# Processed data in batches of num_unit
	num_total = len(df)
	num_files = num_total // num_unit
	if num_total % num_unit > 0:
		num_files += 1

	for i in range(num_files):
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
			answers = get_gpt_answers(
				client=client,
				query=new_query_total[sub_index],
				search_results=search_result_total[sub_index],
				max_tokens=max_tokens,
			)
			answer_list[j] = answers.replace('\n', '')

		df = pd.DataFrame({})
		df['query'] = query_total[start:end] # Retain 'df['query']' as it will be used as the key to merge this dataset with others later.
		df['new_query'] = new_query_total[start:end]
		df['answer'] = answer_list
		df.to_csv(answer_path, index=False)


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


def run_parse_gpt_answers(cwd,idx):
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
