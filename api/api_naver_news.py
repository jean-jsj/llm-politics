import pandas as pd
import os
import urllib.request
import json
import time
import argparse
from dotenv import load_dotenv

load_dotenv()
NAVER_DICT = os.getenv("NAVER_DICT")


def get_news_url(query, display_num):
	base = 'https://openapi.naver.com/v1/search'
	node = '/news.json'
	param_query = '?query=' + urllib.parse.quote(query)
	param_disp = '&display=' + str(display_num)
	param_start = '&start=' + str(1)
	param_sort = '&sort=' + 'sim' 

	return base + node + param_query + param_disp + param_start + param_sort


def get_result(url,client_id,client_secret):
	time.sleep(0.3)
	request = urllib.request.Request(url)
	request.add_header('X-Naver-Client-Id', client_id)
	request.add_header('X-Naver-Client-Secret', client_secret)
	response = urllib.request.urlopen(request)

	return json.loads(response.read().decode('utf-8'))


def get_news_fields(query,json_data):
	query = [query for each in json_data['items']]
	title = [delete_tag(each['title']) for each in json_data['items']]
	originallink = [each['originallink'] for each in json_data['items']]
	naverlink = [each['link'] for each in json_data['items']] 
	description = [delete_tag(each['description']) for each in json_data['items']]
	pubDate = [each['pubDate'] for each in json_data['items']]
	result_news = pd.DataFrame({'query': query, 'title': title, 'originallink': originallink, 'naverlink': naverlink, 'description': description, 'pubDate': pubDate}, columns=['query', 'title', 'originallink', 'naverlink', 'description', 'pubDate'])

	return result_news


def delete_tag(input_str):
	input_str = input_str.replace('<b>', '')
	input_str = input_str.replace('</b>', '')
	input_str = input_str.replace('&quot;', '')
	return input_str


def main(args):
	cwd = os.getcwd()
	idx = args.idx
	client_id, client_secret = NAVER_DICT[idx]

	total_df = pd.DataFrame()
	for i in range(1,41):
		path = os.path.join(
			cwd,
			'google_summary_'+str(i)+'.csv'
		)
		sub_df = pd.read_csv(path)
		total_df = pd.concat([total_df,sub_df],ignore_index=True)
	total_df.drop_duplicates(subset=['query'],inplace=True)

	# NAVER API allows a maximum of 25,000 calls per day
	num_unit = 25000	
	start = idx*num_unit
	end = (idx+1)*num_unit
	idx_df = total_df[start:end]
	query_lst = list(idx_df['query'])

	result_df = pd.DataFrame()
	for query in query_lst:
		try:
			news_url = get_news_url(query,20)
			news_result = get_result(news_url,client_id,client_secret)
			news_df = get_news_fields(query,news_result)
			result_df = pd.concat([result_df,news_df],ignore_index=True)
		except:
			continue
	result_df.to_csv(f'naver_politics_news_{idx}.csv',index=False)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()
	main(args)
