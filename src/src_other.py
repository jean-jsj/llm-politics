import numpy as np
import pandas as pd

def freq(df):
	final_df = df.groupby(['panel','week_num']).size().reset_index(name='freq')
	return final_df


def src_unique(df):
	final_df = df.groupby(['panel','week_num'])['edit_query_proper'].unique().reset_index(name='unique')
	final_df['len_unique'] = final_df['unique'].apply(lambda x: len(x))
	return final_df


def cumulative(df,window_size):
	new_unique = []
	for i,row in df.iterrows():
		if row['week_num'] < window_size:
			new_unique.append(0)
		else:
			past_set = set()
			for j in range(1,window_size):
				if df['unique'][i-j] == []:
					continue
				else:
					past_set.update(set(df['unique'][i-j]))
			
			if row['unique'] == []:
				new_unique.append(np.nan)
			else:
				current_set = set(row['unique'])
				new_unique.append(len(current_set.difference(past_set)))
	
	df['new_unique'] = new_unique
	return df


def unique(df, balanced_df):
	df_unique = src_unique(df)
	balanced_df = pd.merge(balanced_df,df_unique,on=['panel','week_num'],how='left')
	
	# replace NaN with []. fillna() does not work
	balanced_df['unique'] = balanced_df['unique'].fillna("").apply(list)
	final_df = cumulative(balanced_df,8)[['panel','week_num','new_unique']]
	return final_df
