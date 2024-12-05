import os
import pandas as pd
from src import src_political_leaning
from src import src_naver_datalab
from src import src_merge_data
from src import src_popularity
from dotenv import load_dotenv

load_dotenv()
NAVER_DICT = os.getenv("NAVER_DICT")


def main():
	'''
	# identify_final_query() standardizes search queries
	# merge raw data ('nielsen_search.csv') with final query list
    # politic_tag() classifies whether each search query belongs to the politics category
    # calculate_political_leaning() calculates the political leaning of each search query
	'''
	cwd = os.getcwd()
	proper_noun_df = src_merge_data.proper_nouns_to_dataframe(cwd)
	final_query = src_merge_data.identify_final_query(cwd,proper_noun_df)
	final_query.to_csv('final_query.csv',index=False)

	search_df = pd.read_csv('nielsen_search.csv')
	search_df = pd.merge(search_df,final_query,on='query',how='left')
	search_df.to_csv('nielsen_search_final.csv',index=False)

	politic_df = src_political_leaning.politic_tag()
	politic_df.to_csv('political_query_list.csv',index=False)
	leaning_df = src_political_leaning.calculate_political_leaning(politic_df) 
	leaning_df.to_csv('political_leaning.csv',index=False)

	src_naver_datalab.run_api_naver_datalab(NAVER_DICT)
	datalab_df = src_naver_datalab.datalab_preprocess()
	query_popularity = src_popularity.save_sample_popularity(search_df,datalab_df)
	query_popularity.to_csv('sample_popularity.csv',index=False)


if __name__=='__main__':
	main()
