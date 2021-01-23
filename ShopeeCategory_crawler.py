'''
目的：爬取蝦皮三層式分類(有些甚至有第四層)
'''
import requests
import json
from pyquery import PyQuery as pq

url = 'https://seller.shopee.tw/api/v3/category/get_all_category_list/?SPC_CDS=6fee51bd-b639-433c-97b3-cba601760072&SPC_CDS_VER=2&filter=blacklist&version=3.1.0'
headers = {
        'User-Agent': 'UserAgent'
        ,'Cookie': 'cookie'
}
r = requests.get(url,headers=headers)
data = json.loads(r.text)

t=[]
for i in range(2688):
    t.append(data['data']['list'][i])

num=0
main=[]
sec=[]
thr=[]
fir=[]
cur_sec_parent=-1
cur_thr_parent=-1
cur_fir_parent=-1
sec_tmp=[]
thr_tmp=[]
fir_tmp=[]
thr_index=-1
fir_index=-1
id_record_table=[]
has_parent_num={}
have=0
for i in t:
    if(i['parent_id']==0 and i['has_active_children']==True): #第一層
        main.append({i['id']:i['display_name']})
        id_record_table.append(i['id'])
        has_parent_num.update({i['id']:0})   
    elif(i['has_active_children']==True): #第二層or有第四層的第三層
        parent=i['parent_id']
        if(has_parent_num.get(parent)==0): #有第三層的第二層
            if(i['name']!='Default' and parent in id_record_table):
                id_record_table.append(i['id'])
                cur_thr_parent=parent
                if(cur_sec_parent!=parent):
                    cur_sec_parent=parent
                    if(sec_tmp):
                        sec.append(sec_tmp)
                    sec_tmp=[]
                    sec_tmp.append({i['id']:i['display_name']})
                    thr.append([])
                    fir.append([])
                    have=1
                    #thr_index=thr_index+1
                    has_parent_num.update({i['id']:1})
                else:
                    if(have==1):
                        thr_index=thr_index+1
                        sec_tmp.append({i['id']:i['display_name']})
                        has_parent_num.update({i['id']:1})
                        have=0
                    else:
                        sec_tmp.append({i['id']:i['display_name']})
                        has_parent_num.update({i['id']:1})
        elif(has_parent_num.get(parent)==1): #有第四層的第三層
            if(i['name']!='Default' and parent in id_record_table):
                id_record_table.append(i['id'])
                if(cur_thr_parent!=parent):
                    cur_thr_parent=parent
                    if(thr_tmp):
                        thr[thr_index].append(thr_tmp)
                    thr_tmp=[]
                    thr_tmp.append({i['id']:i['display_name']})
                    fir[thr_index].append([])
                    fir_index=fir_index+1
                    has_parent_num.update({i['id']:2})
                else:
                    thr_tmp.append({i['id']:i['display_name']})
                    has_parent_num.update({i['id']:2})
    elif(i['has_active_children']==False and i['has_children']==True): #沒有第三層的第二層
        parent2=i['parent_id']
        if(i['name']!='Default' and parent2 in id_record_table):
            id_record_table.append(i['id'])
            if(cur_sec_parent!=parent2):
                cur_sec_parent=parent2
                if(sec_tmp):
                    sec.append(sec_tmp)
                sec_tmp=[]
                sec_tmp.append({i['id']:i['display_name']})    
                has_parent_num.update({i['id']:1})
                thr.append([])
                fir.append([])
                #thr_index=thr_index+1
            else:
                sec_tmp.append({i['id']:i['display_name']})  
                has_parent_num.update({i['id']:1})
    elif(i['has_active_children']==False and i['has_children']==False): #沒有第四層的第三層or第四層
        parent3=i['parent_id']
        if(has_parent_num.get(parent3)==1): #沒有第四層的第三層
            #if(i['name']!='Default' and parent3!=parent2 and parent3 in id_record_table):
            if(i['name']!='Default' and parent3 in id_record_table):
                id_record_table.append(i['id'])
                if(cur_thr_parent!=parent3):
                    cur_thr_parent=parent3
                    if(thr_tmp):
                        thr[thr_index].append(thr_tmp)
                    thr_tmp=[]
                    thr_tmp.append({i['id']:i['display_name']})
                    fir[thr_index].append([])
                    has_parent_num.update({i['id']:2})
                else:
                    thr_tmp.append({i['id']:i['display_name']})
                    has_parent_num.update({i['id']:2})
        elif(has_parent_num.get(parent3)==2): #第四層
            if(i['name']!='Default' and parent3 in id_record_table):
                id_record_table.append(i['id'])
                if(cur_fir_parent!=parent3):
                    cur_fir_parent=parent3
                    if(fir_tmp):
                        fir[fir_index].append(fir_tmp)
                    fir_tmp=[]
                    fir_tmp.append({i['id']:i['display_name']})
                    has_parent_num.update({i['id']:3})
                else:
                    fir_tmp.append({i['id']:i['display_name']})
                    has_parent_num.update({i['id']:3})
    num=num+1