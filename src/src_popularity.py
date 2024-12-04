import pandas as pd
	

def datalab_popularity(search_df,datalab_df):
	total_df = pd.merge(search_df,datalab_df,on='final_query')
	result = total_df.groupby(['panel','week_num'])['datalab'].mean().reset_index(name='datalab_dup')
	return result


def sample_popularity(search_df):
	duplicate_df = search_df.groupby(['final_query'])['final_query'].count().reset_index(name='duplicates')
	total_df = pd.merge(search_df,duplicate_df,on='final_query')
	result = total_df.groupby(['panel','week_num'])['duplicates'].mean().reset_index(name='avg_dup')
	return result


def sample_popularity_by_search_engines(search_df):
	duplicate_df = search_df.groupby(['search_engine','final_query'])['final_query'].count().reset_index(name='duplicates')
	total_df = pd.merge(search_df,duplicate_df,on=['search_engine','final_query'])
	result = total_df.groupby(['panel','week_num'])['duplicates'].mean().reset_index(name='search_engine_avg_dup')
	return result
