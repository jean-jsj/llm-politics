import re
import time
import pandas as pd


def title_standardize(title):
	title = title.replace('<b>','')
	title = title.replace('</b>','')
	title = title.replace('\xa0','')
	title = title.replace('주식회사','')
	title = title.replace('유한회사','')
	title = title.replace('사단법인','')
	title = title.replace('(주)','')
	title = title.replace('(사)','')
	title = title.replace('(유)','')

	round_pattern = r'\([^)]*\)'
	square_pattern = r'\[.*\]'
	new_entity = re.sub(pattern=round_pattern, repl='', string=title)
	if '[' in new_entity and ']' in new_entity:
		new_entity = re.sub(pattern=square_pattern, repl='', string=new_entity)
	elif '[' in new_entity and ']' not in new_entity:
		new_entity = new_entity.split('[')[0]
	if '<' in new_entity and '>' not in new_entity:
		new_entity = new_entity.split('<')[0]
	
	new_entity = new_entity.replace('<200e>','').strip()
	return new_entity


def art_standardize(title):
	new_entity = title.split(' -')[0].split('/')[0].split('|')[0]
	new_entity = new_entity.replace('다시보기','').replace('검색결과','').replace('의 영화 및 프로그램','').replace('의 기사 모음','').replace('주요정보','')
	new_entity = new_entity.replace('시청','').replace('공식','').replace('매니저','').replace('미리보기','').replace('대표곡','').replace('작곡 컬렉션','')
	new_entity = new_entity.replace('작업곡 모음!!','').replace('작업곡','').replace('등장인물 소개','').replace('탤런트','').replace('보기','').replace('<200e>','').strip()
	# remove episode
	if len(new_entity) > 2:
		if str(new_entity[-1]) == "화" and new_entity[-2].isnumeric() == True:
			last_occurence = int(new_entity.rfind(' '))
			new_entity = new_entity[:last_occurence]
	return new_entity


def wiki_standardize(title):
	if ' - 위키백과' in title or ' - 나무위키' in title or ' - Wikipedia' in title:
		title = title.split(' - ')[0]
	if '논란' in title:
		title = title.replace('논란','')
	
	title = title.strip()
	return title


def find_art(link, title):
	arts = [
	"ypbooks.", "kyobobook.", "barnesandnoble.", "goodreads.", "www.penguin.", "seoulbookbogo",
	"ridibooks.","book.interpark.","www.aladin.","webtoon.kakao.","series.naver.", "nl.go.kr",
	"www.genie.", "mw.genie.", "spotify.", "music.bugs.",
	"m.bugs.", "m2.melon.", "music.apple.", "music.youtube.",
	"programs.sbs.", "themoviedb.", "serieson.naver.", "rottentomatoes.", "imdb.", "kmdb.",
	"netflix.", "cgv.", "megabox.", "tving.", "tv.apple.", "tvn.cjenm.com", "lezhin.",
	"disneyplus.", "tv.naver.", "tv.kakao.", "watcha.", "coupangplay.", "wavve.",
	]
	for art in arts:
		if art in link:
			title = title_standardize(title)
			response = {
				'title': art_standardize(title),
			}
			return response
	return -1


def find_wiki(link, title):
	wikis = [
		"namu.wiki", "wikipedia"
	]
	for wiki in wikis:
		if wiki in link:
			edit_title = wiki_standardize(title)
			response = {
				'title': edit_title,
				'category': 'wiki'
			}
			return response
	return -1


def get_subset_organic(df, query):
	condition = (df['query'] == query)
	df_subset = df[condition]
	return df_subset


def run_standardization(
		df_summary,
		df_organic,
		df_knowledgeGraph,
		df_answerBox,
		df_places,
		output_path,
	):
	st = time.time()
	
	query_list = []
	edit_query_list = []

	query_list = list(df_summary['query'])
	for i, query in enumerate(query_list):
		query = str(query)
		edit_query = ''
		flag_query = False
		
		if i % 1000 == 0:
			et = time.time()
			print (i, round(et-st, 2), "(s)")
		
		row = df_summary.iloc[i]
		is_ab = int(row['answerBox'])
		is_kg = int(row['knowledgeGraph'])
		is_pl = int(row['places'])

		if is_kg == 1:
			condition = (df_knowledgeGraph['query'] == query)
			df_ = df_knowledgeGraph[condition]
			if len(df_) > 0:
				for i in range(len(df_)):
					row = df_.iloc[i]
					title = str(row['title']) 
					if title != 'nan':
						edit_query = title
						flag_query = True
						break
		
		if is_ab == 1:
			condition = (df_answerBox['query'] == query)
			df_ = df_answerBox[condition]
			if len(df_) > 0:
				for i in range(len(df_)):
					row = df_.iloc[i]
					title = str(row['title'])
					if title != 'nan':
						new_title = title_standardize(title)
						edit_query = new_title.split(' - ')[0].split('|')[0].strip()
						flag_query = True
						break

		if is_pl == 1:
			condition = (df_places['query'] == query)
			df_ = df_places[condition]
			if len(df_) > 0:
				for i in range(len(df_)):
					row = df_.iloc[i]
					title = str(row['title']) 
					if title != 'nan':
						edit_query = title
						flag_query = True
						break
					
		df_subset_organic = get_subset_organic(df_organic, query)
		for j in range(len(df_subset_organic)):
			row = df_subset_organic.iloc[j]
			link = str(row['link'])
			title = str(row['title'])
	
			response = find_wiki(link=link, title=title)
			if response != -1:
				edit_query = response['title']
				flag_query = True
				break
	
			response = find_art(link=link, title=title)
			if response != -1:
				edit_query = response['title']
				flag_query = True
				break

		query_list.append(query)
		edit_query_list.append(edit_query)

	df_result = pd.DataFrame()
	df_result['query'] = query_list
	df_result['edit_query'] = edit_query_list
	df_result.to_csv(output_path, index=False)

	et = time.time()
	print ("Time for standardization:", round(et-st, 2), "(s)")

