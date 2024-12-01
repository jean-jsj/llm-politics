import time
import urllib.request
import argparse
import json
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
NAVER_DICT = os.getenv("NAVER_DICT")

class NaverDataLab():
	def __init__(self, client_id, client_secret):
		self.client_id = client_id
		self.client_secret = client_secret
		self.keywordGroups = []
		self.url = "https://openapi.naver.com/v1/datalab/search"
	
	def add_keywordGroup(self, group_dict):
		keyword_group = {
			'groupName': group_dict['groupName'],
			'keywords': group_dict['keywords']
		}

		self.keywordGroups.append(keyword_group)

	def naver_datalab_api(self,startDate,endDate,timeUnit,device,ages,gender):	
		body = json.dumps({
			"startDate": startDate,
			"endDate": endDate,
			"timeUnit": timeUnit,
			"keywordGroups": self.keywordGroups,
			"device": device,
			"ages": ages,
			"gender": gender
		}, ensure_ascii=False)
	
		request = urllib.request.Request(self.url)
		request.add_header("X-Naver-Client-Id",self.client_id)
		request.add_header("X-Naver-Client-Secret",self.client_secret)
		request.add_header("Content-Type", "application/json")
		response = urllib.request.urlopen(request, data=body.encode('utf-8'))
		rescode = response.getcode()
		if(rescode==200):
			response_body = response.read()
			response_json = json.loads(response_body)
			
			df1 = pd.DataFrame(response_json['results'][1]['data'])
			df1['query'] = response_json['results'][1]['title']
			df2 = pd.DataFrame(response_json['results'][2]['data'])
			df2['query'] = response_json['results'][2]['title']
			df3 = pd.DataFrame(response_json['results'][3]['data'])
			df3['query'] = response_json['results'][3]['title']
			df4 = pd.DataFrame(response_json['results'][4]['data'])
			df4['query'] = response_json['results'][4]['title']
			df = pd.concat([df1,df2,df3,df4],ignore_index=True)

		else:
			print('error code: '+rescode)	
		
		return df

def main(args):
	idx = args.idx
	startDate = "2022-09-01"
	endDate = "2023-09-01"
	timeUnit = 'month'
	device = ''
	ages = []
	gender = ''
	
	df = pd.read_csv('data.csv')
	query_list = list(df['edit_query'])
	total_len = len(query_list)

	# NAVER API allows a maximum of 25,000 calls per day -> Limit each idx to 20,000 calls per day, making 10 separate batches of 2,000 calls each.
	for i in range(10):
		print('num_unit: ',i)
		start = i*2000
		end = (i+1)*2000
		if end > total_len:
			end = total_len
		query = query_list[start:end]
		client_id, client_secret = NAVER_DICT[i]
		response_results_all = pd.DataFrame()

		for j in range(500):
			time.sleep(0.3)
			keyword1 = query[(j*4)]
			keyword2 = query[(j*4)+1]
			keyword3 = query[(j*4)+2]
			keyword4 = query[(j*4)+3]
			keyword_group_set = {
				'keyword_group_1': {'groupName': "경복궁", 'keywords': ["경복궁"]},
				'keyword_group_2': {'groupName': f"{keyword1}", 'keywords': [f"{keyword1}"]},
				'keyword_group_3': {'groupName': f"{keyword2}", 'keywords': [f"{keyword2}"]},
				'keyword_group_4': {'groupName': f"{keyword3}", 'keywords': [f"{keyword3}"]},
				'keyword_group_5': {'groupName': f"{keyword4}", 'keywords': [f"{keyword4}"]},
			}
			naver = NaverDataLab(client_id=client_id,client_secret=client_secret)
			naver.add_keywordGroup(keyword_group_set['keyword_group_1'])
			naver.add_keywordGroup(keyword_group_set['keyword_group_2'])
			naver.add_keywordGroup(keyword_group_set['keyword_group_3'])
			naver.add_keywordGroup(keyword_group_set['keyword_group_4'])
			naver.add_keywordGroup(keyword_group_set['keyword_group_5'])
	
			response_results = naver.naver_datalab_api(startDate,endDate,timeUnit,device,ages,gender)
			response_results_all = pd.concat([response_results_all,response_results],ignore_index=True)
		
		path = 'datalab_naver_'+str(idx)+'_'+str(i)+'.csv'
		response_results_all.to_csv(path,index=False)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--idx', type=int, required=True,
						help='idx')
	args = parser.parse_args()
	main(args)
