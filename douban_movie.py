import requests
import pandas as pd
from bs4 import BeautifulSoup
import json
import re

#初始化变量
list_url = 'https://movie.douban.com/j/search_subjects?type=movie&tag=%E8%B1%86%E7%93%A3%E9%AB%98%E5%88%86&sort=rank&page_limit=500&page_start=0'
url = 'https://movie.douban.com/subject/1292052/?tag=%E8%B1%86%E7%93%A3%E9%AB%98%E5%88%86&from=gaia_video'


def get_movie_list(url):
    
    res = requests.get(url)
    df = pd.DataFrame(json.loads(res.text)['subjects'])
    return df


def get_movie_info(url):           #获取具体某一电影的详细信息

    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36',
        'Connection': 'keep-alive',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'    ,
        'Referer': 'https://www.douban.com/'
    }

    res = requests.get(url = url, headers=header)
    soup = BeautifulSoup(res.text,'html.parser')
    content = soup.select('#info span')

    movie_info = {}

    #获取详细字段
    movie_info['director'] = soup.select('#info [rel="v:directedBy"]')[0].text                  #导演
    movie_info['scriptwriter'] = content[5].text.strip().split('/')                              #编剧
    movie_info_actor = soup.select('#info [rel="v:starring"]')                                   #演员，多个演员，需获取列表
    movie_info['actor']= [] 
    for i in movie_info_actor:
        movie_info['actor'].append(i.text)

    movie_info_type = soup.select('#info [property="v:genre"]')                                  #类型，可能属于多种类型
    movie_info['type']= [] 
    for i in movie_info_type:
        movie_info['type'].append(i.text)

    movie_info['runtime'] = soup.select('#info [property="v:runtime"]')[0].text                  #时长
  
    movie_info['launch_time'] = soup.select('#info [property="v:initialReleaseDate"]')[0].text if  soup.select('#info [property="v:initialReleaseDate"]') else '无'   #上映日期
        
    county_pattern = re.compile(r'<span class="pl">制片国家/地区:</span>(.*)<br/>')              #国家
    movie_info['country'] = re.findall(county_pattern,str(soup))[0].strip()                      #没有标签包围，用正则式提取

    language_pattern = re.compile(r'<span class="pl">语言:</span>(.*)<br/>')                     #语言
    movie_info['language'] = re.findall(language_pattern,str(soup))[0].strip()                   #没有标签包围，用正则式提取
    
    movie_info['summary'] = soup.select('[property="v:summary"]')[0].text.strip() if  soup.select('[property="v:summary"]') else '无'                      #简介
    
    return movie_info

def merge_movie_info(df):
    
    data = {}                                #每个电影的详细信息记录到字典里
    data_list = []                           #所有电影的详情存放到列表中
    count = 1
    for i in df['id']:
        data = {}
        url = 'https://movie.douban.com/subject/' + str(i)
        info = get_movie_info(url)
        data['id'] = i
        data['director'] = info['director']
        data['scriptwriter'] = info['scriptwriter']
        data['actor'] = info['actor']
        data['type'] = info['type']
        data['runtime'] = info['runtime']
        data['launch_time'] = info['launch_time']
        data['country'] = info['country']
        data['language'] = info['language']
        data['summary'] = info['summary']
        data_list.append(data)
        print('第' + str(count) +'条数据已加载,已下载' + str(count * 100/len(df)) + '%的数据' + url)
        count += 1

    final_data = pd.merge(df,pd.DataFrame(data_list))  
    return final_data
        

df = get_movie_list(list_url)                           #获取豆瓣排名前200电影列表
info_data = merge_movie_info(df)                         #根据每个电影ID获取每个电影的详细信息
info_data_df = pd.DataFrame(info_data)
info_data_df.to_csv('douban_movie_top500.csv')
print('数据加载完成')
