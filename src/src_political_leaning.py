import pandas as pd
import glob

def clean(news):
	news = news.replace('https://','').replace('http://','').replace('www.','')
	if '.co' in news:
		news = news.split('.co',1)[0]
	elif '.kr' in news:
		news = news.split('.kr',1)[0]
	elif '.net' in news:
		news = news.split('.net',1)[0]
	elif '.org' in news:
		news = news.split('.org',1)[0]
	return news 


def political_news_domain():
	'''
	lists up the news domains included in the dataset
	'''
	df = pd.DataFrame()
	path_files = glob.glob('./naver_politics_news_*.csv')
	for path in path_files:
		sub_df = pd.read_csv(path)
		df = pd.concat([df,sub_df],ignore_index=True)
	
	links = set()
	for i,row in df.iterrows():
		element = str(row['naverlink'])
		if 'news.naver.com' in element:
			if 'sid=100' in element:
				news = str(row['news_domain'])
				element = clean(news)
				links.add(element)
	print(links)


def politic_tag():
	'''
	classify whether each search query belongs to the politics category
	'''
	df = pd.DataFrame()
	path_files = glob.glob('./naver_politics_news_*.csv')	
	for path in path_files:
		sub_df = pd.read_csv(path)
		politics = []
		for i,row in sub_df.iterrows():
			element = str(row['naverlink'])
			tag = 0
			if 'news.naver.com' in element:
				if 'sid=100' in element:
					tag = 1
			politics.append(tag)
		sub_df['politics'] = politics
		sum_df = sub_df.groupby('query')['politics'].sum().reset_index(name='total')
		
		tag_politic = []
		for i,row in sum_df.iterrows():
			if row['total'] > 4:
				tag_politic.append(1)
			else:
				tag_politic.append(0)
		sum_df['tag_politic'] = tag_politic
		sum_df = sum_df[sum_df['tag_politic']==1]
		df = pd.concat([df,sum_df],ignore_index=True)
	return df


def no_weight_leaning(sub_df):
	left = 0
	right = 0
	neutral = 0
	for i,row in sub_df.iterrows():
		news = str(row['news_domain'])
		element = clean(news)
		if ('nongmin' in element) or ('justice21' in element) or ('khan' in element) or ('sisapress' in element) or ('sisain' in element) or ('newstapa' in element) or ('pressian' in element) or ('nocutnews') in element or ('cbs' in element) or ('jtbc' in element) or ('mbc' in element) or ('moneytoday' in element) or ('mt' in element) or ('ohmynews' in element) or ('hani' in element) or ('mediatoday' in element) or ('edaily' in element) or ('justiceon' in element) or ('news1' in element) or ('ildaro' in element):
			left += 1
		elif ('dailian' in element) or ('joins' in element) or ('hankookilbo' in element) or ('hankyung' in element) or ('mk' in element) or ('chosun' in element) or ('donga' in element) or ('sisajournal' in element) or ('munhwa' in element) or ('joongang' in element) or ('kukinews' in element) or ('segye' in element) or ('sedaily' in element) or ('imaeil' in element) or ('kmib' in element) or ('joseilbo' in element) or ('ichannela' in element) or ('uberin' in element) or ('etnews' in element):
			right += 1
		else:
			neutral += 1
	total = left + right + neutral
	return (left*(-1) + right*1 + neutral*0)/total


def weight_leaning(sub_df):
	leaning_list = []
	for i,row in sub_df.iterrows():
		news = str(row['news_domain'])
		element = clean(news)
		if ('nongmin' in element) or ('justice21' in element) or ('khan' in element) or ('sisapress' in element) or ('sisain' in element) or ('newstapa' in element) or ('pressian' in element) or ('nocutnews') in element or ('cbs' in element) or ('jtbc' in element) or ('mbc' in element) or ('moneytoday' in element) or ('mt' in element) or ('ohmynews' in element) or ('hani' in element) or ('mediatoday' in element) or ('edaily' in element) or ('justiceon' in element) or ('news1' in element) or ('ildaro' in element):
			leaning_list.append(-1)
		elif ('dailian' in element) or ('joins' in element) or ('hankookilbo' in element) or ('hankyung' in element) or ('mk' in element) or ('chosun' in element) or ('donga' in element) or ('sisajournal' in element) or ('munhwa' in element) or ('joongang' in element) or ('kukinews' in element) or ('segye' in element) or ('sedaily' in element) or ('imaeil' in element) or ('kmib' in element) or ('joseilbo' in element) or ('ichannela' in element) or ('uberin' in element) or ('etnews' in element):
			leaning_list.append(1)
		else:
			leaning_list.append(0)	
	denominator = [item for item in range(1,len(leaning_list)+1)]

	weighted_leaning = 0
	for i in range(len(leaning_list)):
		weighted_leaning += (leaning_list[i] / denominator[i])
	return weighted_leaning


def no_weight_no_neutral_leaning(sub_df):
	left = 0
	right = 0
	neutral = 0
	for i,row in sub_df.iterrows():
		news = str(row['news_domain'])
		element = clean(news)
		if ('nongmin' in element) or ('justice21' in element) or ('khan' in element) or ('sisapress' in element) or ('sisain' in element) or ('newstapa' in element) or ('pressian' in element) or ('nocutnews') in element or ('cbs' in element) or ('jtbc' in element) or ('mbc' in element) or ('moneytoday' in element) or ('mt' in element) or ('ohmynews' in element) or ('hani' in element) or ('mediatoday' in element) or ('edaily' in element) or ('justiceon' in element) or ('news1' in element) or ('ildaro' in element):
			left += 1
		elif ('dailian' in element) or ('joins' in element) or ('hankookilbo' in element) or ('hankyung' in element) or ('mk' in element) or ('chosun' in element) or ('donga' in element) or ('sisajournal' in element) or ('munhwa' in element) or ('joongang' in element) or ('kukinews' in element) or ('segye' in element) or ('sedaily' in element) or ('imaeil' in element) or ('kmib' in element) or ('joseilbo' in element) or ('ichannela' in element) or ('uberin' in element) or ('etnews' in element):
			right += 1
		else:
			neutral += 1
	total = left + right

	if (total == 0) & (neutral > 0):
		return 0
	return (left*(-1) + right*1)/total


def weight_no_neutral_leaning(sub_df):
	leaning_list = []
	for i,row in sub_df.iterrows():
		news = str(row['news_domain'])
		element = clean(news)
		if ('nongmin' in element) or ('justice21' in element) or ('khan' in element) or ('sisapress' in element) or ('sisain' in element) or ('newstapa' in element) or ('pressian' in element) or ('nocutnews') in element or ('cbs' in element) or ('jtbc' in element) or ('mbc' in element) or ('moneytoday' in element) or ('mt' in element) or ('ohmynews' in element) or ('hani' in element) or ('mediatoday' in element) or ('edaily' in element) or ('justiceon' in element) or ('news1' in element) or ('ildaro' in element):
			leaning_list.append(-1)
		elif ('dailian' in element) or ('joins' in element) or ('hankookilbo' in element) or ('hankyung' in element) or ('mk' in element) or ('chosun' in element) or ('donga' in element) or ('sisajournal' in element) or ('munhwa' in element) or ('joongang' in element) or ('kukinews' in element) or ('segye' in element) or ('sedaily' in element) or ('imaeil' in element) or ('kmib' in element) or ('joseilbo' in element) or ('ichannela' in element) or ('uberin' in element) or ('etnews' in element):
			leaning_list.append(1)
	denominator = [item for item in range(1,len(leaning_list)+1)]
	if len(denominator) == 0:
		return 0
		
	weighted_leaning = 0
	for i in range(len(leaning_list)):
		weighted_leaning += (leaning_list[i] / denominator[i])
	return weighted_leaning

	

def calculate_political_leaning(politic_df):	
	'''
	# calculates the political leaning of each search query using four approaches:  
	1) Aggregate data at the query level based on ranking weights; include neutral news domains
	2) Aggregate data to the query level by calculating the average values; include neutral news domains
	3) Aggregate data at the query level based on ranking weights; DO NOT include neutral news domains
	4) Aggregate data to the query level by calculating the average values; DO NOT include neutral news domains
	'''
	df = pd.DataFrame()
	path_files = glob.glob('./naver_politics_news_*.csv')
	for path in path_files:
		sub_df = pd.read_csv(path)		
		df = pd.concat([df,sub_df],ignore_index=True)
	
	query_lst = list(set(politic_df['query']))
	weight_lst = []
	no_weight_lst = []
	weight_no_neutral_lst = []
	no_weight_no_neutral_lst = []

	for query in query_lst:
		sub_query_df = df[df['query']==query]
		no_weight = no_weight_leaning(sub_query_df)
		weight = weight_leaning(sub_query_df)
		no_weight_no_neutral = no_weight_no_neutral_leaning(sub_query_df)
		weight_no_neutral = weight_no_neutral_leaning(sub_query_df)
		
		weight_lst.append(weight)
		no_weight_lst.append(no_weight)
		weight_no_neutral_lst.append(weight_no_neutral)
		no_weight_no_neutral_lst.append(no_weight_no_neutral)

	df_1 = pd.DataFrame(list(zip(query_lst,weight_lst)), columns=['query','w_leaning'])
	df_2 = pd.DataFrame(list(zip(query_lst,no_weight_lst)), columns=['query','nw_leaning'])
	df_3 = pd.DataFrame(list(zip(query_lst,weight_no_neutral_lst)), columns=['query','w_nn_leaning'])
	df_4 = pd.DataFrame(list(zip(query_lst,no_weight_no_neutral_lst)), columns=['query','nw_nn_leaning'])

	result_df = pd.merge(df_1,df_2,on='query',how='outer')
	result_df = pd.merge(result_df,df_3,on='query',how='outer')
	result_df = pd.merge(result_df,df_4,on='query',how='outer')
	
	return result_df
	

def user_level_leaning(search_df,politic_df):
	# 'naver_politics_news_*.csv' was created using the NAVER NEWS API -> Each search query is paired with its 10 most relevant news articles
	path_files = glob.glob('./naver_politics_news_*.csv')
	news_df = pd.DataFrame()
	for path in path_files:
		sub_df = pd.read_csv(path)		
		news_df = pd.concat([news_df,sub_df],ignore_index=True)
	news_df = news_df[['query','news_domain']]
	news_df['news_domain'] = news_df['news_domain'].apply(str).apply(clean)

	# Merge with politic_df to retain only the political search queries
	news_df = pd.merge(news_df,politic_df,on='query',how='right')

	# The goal is to identify the most common news domain for each panel-week:
	# 1. For each search query, count how many times each news domain appears across its associated news articles.
	# 2. For each panel-week, count how many times each search query was executed (note: a search query may be executed multiple times).
	# 3. For each panel-week, calculate the total appearances of each news domain by multiplying 1. and 2.
	# 4. Identify the news domain(s) with the highest domain_cnt value for each panel-week. Note: Multiple news domains may have the same domain_cnt value.
	result_df = news_df.groupby(['query','news_domain'])['news_domain'].count().reset_index(name='link_cnt')
	search_df = search_df.groupby(['panel','week_num','query'])['query'].count().reset_index(name='query_cnt')
	search_df = pd.merge(search_df,result_df,on='query',how='right')
	search_df['cnt'] = search_df['link_cnt'] * search_df['query_cnt']
	user_df = search_df.groupby(['panel','week_num','news_domain'])['cnt'].sum().reset_index(name='domain_cnt')	
	user_df_max = user_df.groupby(['panel','week_num'])['domain_cnt'].max().reset_index()
	final_df = pd.merge(user_df,user_df_max,on=['panel','week_num','domain_cnt'],how='right')

	leaning = []
	for i,row in final_df.iterrows():
		element = str(row['news_domain'])
		if ('nongmin' in element) or ('justice21' in element) or ('khan' in element) or ('sisapress' in element) or ('sisain' in element) or ('newstapa' in element) or ('pressian' in element) or ('nocutnews') in element or ('cbs' in element) or ('jtbc' in element) or ('mbc' in element) or ('moneytoday' in element) or ('mt' in element) or ('ohmynews' in element) or ('hani' in element) or ('mediatoday' in element) or ('edaily' in element) or ('justiceon' in element) or ('news1' in element) or ('ildaro' in element):
			leaning.append(-1)
		elif ('dailian' in element) or ('joins' in element) or ('hankookilbo' in element) or ('hankyung' in element) or ('mk' in element) or ('chosun' in element) or ('donga' in element) or ('sisajournal' in element) or ('munhwa' in element) or ('joongang' in element) or ('kukinews' in element) or ('segye' in element) or ('sedaily' in element) or ('imaeil' in element) or ('kmib' in element) or ('joseilbo' in element) or ('ichannela' in element) or ('uberin' in element) or ('etnews' in element):
			leaning.append(1)
		else:
			leaning.append(0)
	final_df['user_exposed_leaning'] = leaning

	# Aggregate data at the panel-week level by computing the average for each group
	result = final_df.groupby(['panel','week_num'])['user_exposed_leaning'].mean().reset_index()
	return result