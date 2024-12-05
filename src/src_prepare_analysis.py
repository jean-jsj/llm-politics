import pandas as pd
from src import src_popularity
from src import src_political_leaning
from src import src_other
 
def main():
	search_df = pd.read_csv('nielsen_search_final.csv')
	datalab_df = pd.read_csv('datalab_popularity')
	politic_df = pd.read_csv('political_query_list.csv')
	leaning_df = pd.read_csv('political_leaning.csv')
	
	panel_lst = []
	week_lst = []
	for panel in list(set(search_df['panel'])):
		panel_lst.extend([panel for _ in range(1,54)])
		week_lst.extend([i for i in range(1,54)])
	balanced_df = pd.DataFrame(list(zip(panel_lst,week_lst)),columns=['panel','week_num'])

	# search frequency, new search
	freq_df = src_other.freq(search_df)
	unique_df = src_other.unique(search_df, balanced_df)
	balanced_df = pd.merge(balanced_df,freq_df,on=['panel','week_num'],how='left')			
	balanced_df = pd.merge(balanced_df, unique_df,on=['panel','week_num'], how='left')
	balanced_df = balanced_df.fillna(0)

	# popularity
	datalab_df =src_popularity.datalab_popularity(search_df,datalab_df)
	popularity_df = src_popularity.sample_popularity(search_df)
	popularity_se_df = src_popularity.sample_popularity_by_search_engines(search_df)
	balanced_df = pd.merge(balanced_df,datalab_df,on=['panel','week_num'],how='left')	
	balanced_df = pd.merge(balanced_df,popularity_df,on=['panel','week_num'],how='left')
	balanced_df = pd.merge(balanced_df,popularity_se_df,on=['panel','week_num'],how='left')
	 
	# political leaning
	search_politic_df = pd.merge(search_df,leaning_df,on='query')
	avg_w_leaning = search_politic_df.groupby(['panel','week_num'])['w_leaning'].mean().reset_index(name='avg_w_leaning')
	avg_nw_leaning = search_politic_df.groupby(['panel','week_num'])['w_leaning'].mean().reset_index(name='avg_nw_leaning')
	avg_w_nn_leaning = search_politic_df.groupby(['panel','week_num'])['w_leaning'].mean().reset_index(name='avg_w_nn_leaning')
	avg_nw_nn_leaning = search_politic_df.groupby(['panel','week_num'])['w_leaning'].mean().reset_index(name='avg_nw_nn_leaning')
	user_leaning = src_political_leaning.user_level_leaning(search_df,politic_df)
	balanced_df = pd.merge(balanced_df,avg_w_leaning,on=['panel','week_num'],how='left')
	balanced_df = pd.merge(balanced_df,avg_nw_leaning,on=['panel','week_num'],how='left')
	balanced_df = pd.merge(balanced_df,avg_w_nn_leaning,on=['panel','week_num'],how='left')
	balanced_df = pd.merge(balanced_df,avg_nw_nn_leaning,on=['panel','week_num'],how='left')
	balanced_df = pd.merge(balanced_df,user_leaning,on=['panel','week_num'],how='left')

	balanced_df.to_csv('analysis.csv',index=False)
	

if __name__=='__main__':
	main()
