def proper_nouns_to_dataframe(cwd):
	'''
    'gpt_answer_edit_{idx}.csv' contains a comma-separated list of proper nouns extracted from each search query.  
    The dataframe is reshaped so that each row contains only one proper noun.  
    If a search query has multiple extracted proper nouns, the final dataset will contain multiple rows for the same search query.
	'''
	total_df = pd.DataFrame()
	for idx in range(1,41):
		path = os.path.join(
			cwd,
			str(idx),
			'gpt_answer_edit_'+str(idx)+'.csv'
		)
		sub_df = pd.read_csv(path)
		total_df = pd.concat([total_df,sub_df])	
	total_df = total_df.drop_duplicates(subset=['query'],inplace=True)
	
	query_list_total = []
	proper_noun_list_total = []	
	for i,row in total_df.iterrows():
		if pd.isna(row['ProperNouns']):
			query_list_total.append(row['query'])
			proper_noun_list_total.append('')
		else:
			query_list = []
			proper_noun_list = []
			if ", " in row['ProperNouns']:
				elements = row['ProperNouns'].split(", ")
				for element in elements:
					query_list.append(row['query'])
					proper_noun_list.append(element)
				query_list_total.extend(query_list)
				proper_noun_list_total.extend(proper_noun_list)
			else:
				query_list_total.append(row['query'])
				proper_noun_list_total.append(row['ProperNouns'])

	proper_noun_df = pd.DataFrame(list(zip(query_list_total,proper_noun_list_total)), columns = ['query','ProperNouns'])
	return proper_noun_df


def identify_final_query(cwd,proper_noun_df):
	'''
    The search queries in the raw data are standardized through two steps:
    (1) Proper noun(s) are extracted from the search query using the run_gpt.py script.
    (2) The run_standardize.py script checks if the search query's Google search results include sources like Google Knowledge Graph or Wikipedia. 
        If they do, the search query is replaced with the title from those sources.
	'''
	edit_df = pd.DataFrame()
	for idx in range(1,41):
		edit_path = os.path.join(
			cwd,
			str(idx),
			'standardize_'+str(idx)+'.csv'
		)
		sub_df = pd.read_csv(edit_path)
		edit_df = pd.concat([edit_df,sub_df])	
	df = pd.merge(edit_df,proper_noun_df,on='query',how='outer')
	df['final_query'] = ['' for _ in range(len(df))]

	for i,row in df.iterrows():
		if not pd.isna(row['edit_query']):
			df.at[i,'final_query'] = row['edit_query']
		elif not pd.isna(row['ProperNouns']):
			df.at[i,'final_query'] = row['ProperNouns']
		else:
			df.at[i,'final_query'] = row['query']
			
	# If a search query has multiple extracted proper nouns, the final dataset will contain multiple rows for the same search query
	df = df.drop_duplicates(subset=['query'],inplace=True)
	return df