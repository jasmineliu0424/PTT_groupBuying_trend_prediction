'''
目的：爬取PTT團購版資訊並將團購標題做斷詞處理，再將處理完的結果放入蝦皮進行分類
'''
import requests
import json
from pyquery import PyQuery as pq
from ckiptagger import WS, POS, NER
from datetime import datetime
import mysql.connector

connection = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="passward",
  database="DB_name"
)

mycursor = connection.cursor(buffered=True)

ws = WS("./data")
pos = POS("./data")
ner = NER("./data")

res = requests.get("https://www.pttweb.cc/bbs/BuyTogether/page?n=102725")
mainPage = pq(res.text)
mainPage.make_links_absolute(base_url=res.url) 
num=102725
List=[]


#將斷詞結果丟入蝦皮搜尋，取前兩個最相關的商品類別作為該團購商品的類別
def cluster(Na, item,Org_and_Product):
    Na=Na.replace(' ','%20')
    if Na[-3:]=='%20':
        Na=Na[:-3]
    if Na[:3]=='%20':
        Na=Na[3:]
    Na=Na.replace('%20%20','%20')
    
    headers = {
        'User-Agent': 'UserAgent',
    }
    url = 'https://shopee.tw/api/v0/search/api/facet/?keyword='+Na+'&page_type=search'
    r = requests.get(url,headers=headers)
    data = json.loads(r.text)
    end=0
    for i in range(2):
        try:
            if data['facets'][i]['category']['display_name']=='其它' or data['facets'][i]['category']['display_name']=='其他類別' or data['facets'][i]['category']['display_name']=='default' or data['facets'][i]['category']['display_name']=='Default':
                i=i+1
            print("-> ",data['facets'][i]['category']['display_name'],"id: ",data['facets'][i]['category']['catid'])
            tmp_id=data['facets'][i]['category']['catid']
            mycursor.execute("SELECT * FROM category WHERE (fir_catid="+str(data['facets'][i]['category']['catid'])+") OR (sec_catid="+str(data['facets'][i]['category']['catid'])+") OR (thr_catid="+str(data['facets'][i]['category']['catid'])+");")
            for (id, fir_catid,fir_category,sec_catid,sec_category,thr_catid,thr_category) in mycursor:
                if(int(fir_catid)==int(tmp_id)):
                    #mycursor.execute("insert into Product_v2 (title,time,people,fir_catid,layerOne_category,sec_catid,layerTwo_category,thr_catid,layerThr_category) values ("+"'"+str(item['title'])+"'"+","+"'"+str(item['time'])+"'"+","+str(item['people'])+","+str(fir_catid)+","+"'"+fir_category+"'"+",NULL,NULL,NULL,NULL);")
                    #connection.commit()
                    break
                elif(int(sec_catid)==int(tmp_id)):
                    #mycursor.execute("insert into Product_v2 (title,time,people,fir_catid,layerOne_category,sec_catid,layerTwo_category,thr_catid,layerThr_category) values ("+"'"+str(item['title'])+"'"+","+"'"+str(item['time'])+"'"+","+str(item['people'])+","+str(fir_catid)+","+"'"+fir_category+"'"+","+str(sec_catid)+","+"'"+sec_category+"'"+",NULL,NULL);")
                    #connection.commit()
                    break
                elif(int(thr_catid)==int(tmp_id)):
                    #mycursor.execute("insert into Product_v2 (title,time,people,fir_catid,layerOne_category,sec_catid,layerTwo_category,thr_catid,layerThr_category) values ("+"'"+str(item['title'])+"'"+","+"'"+str(item['time'])+"'"+","+str(item['people'])+","+str(fir_catid)+","+"'"+fir_category+"'"+","+str(sec_catid)+","+"'"+sec_category+"'"+","+str(thr_catid)+","+"'"+thr_category+"'"+");")
                    #connection.commit()
                    break
            end+=1
        except:
            break
    if(end>0):
        end=0
    else:
        end=1
    return end

# 將PTT團購標題做斷詞，並將斷詞結果拿去分類
def ckip(title, item_type, item):
    special_fw=['youtube','netflix 會員','netfilx 會員','spotify 會員','switch 會員','office365']
    valid_type=['寵物','票券']
    title=title.lower()
    only_Na=[] #只存放Na
    Org_and_Product={} #存放ORG和Product
    foreign_word=[] #只存放FW
    Na=""
    deleteNa=[] #欲刪除之字串
    lenDelete=[]
    Length=len(title)
    ws_results = ws([title])
    pos_results = pos(ws_results)
    ner_results = ner(ws_results, pos_results)
    if('-' in title):
        #擷取'-'前面的字串
        dashIndex=title.rfind('-')
        title=title[0:dashIndex]
        Length=len(title)
        ws_results = ws([title])
        pos_results = pos(ws_results)
        ner_results = ner(ws_results, pos_results)
    elif('/' in title):
        #擷取'/'前面的字串
        slashIndex=title.find('/')
        title=title[0:slashIndex]
        Length=len(title)
        ws_results = ws([title])
        pos_results = pos(ws_results)
        ner_results = ner(ws_results, pos_results)
    if('(' in title):
        #擷取'('前面的字串
        bracketIndex=title.find('(')
        title=title[0:bracketIndex]
        Length=len(title)
        ws_results = ws([title])
        pos_results = pos(ws_results)
        ner_results = ner(ws_results, pos_results)
    #取ORG PRODUCT(查無商品時再使用)
    #去除DATE
    if(len(ner_results[0])!=0):
        for name in ner_results[0]:
            if('ORG' in name):
                Org_and_Product['ORG']=name[3]
            if('PRODUCT' in name):                    
                Org_and_Product['PRODUCT']=name[3]
            if('DATE' in name):
                deleteNa.append(name[3])
                lenDelete.append(len(name[3]))
        for i in range(len(deleteNa)):
            p=title.rfind(deleteNa[i])
            l=len(deleteNa[i])
            title=title[0:p]+title[(p+l):Length]
            Length=len(title)
            ws_results = ws([title])
            pos_results = pos(ws_results)
            ner_results = ner(ws_results, pos_results)
    #去除'人'與人前的數字
    if('人' in title):
        peopleIndex=0
        for i in ws_results[0]:
            if('人' in i):
                break
            else:
                peopleIndex+=1
        if(peopleIndex-1>=0):
            if(pos_results[0][peopleIndex-1]=='Neu' and pos_results[0][peopleIndex]=='Na'):
                numLen=len(ws_results[0][peopleIndex-1])
                peoLen=1
                words_after_Na=0
                for t in ws_results[0]:
                    if(ws_results[0].index(t)>peopleIndex):
                        words_after_Na+=len(t)
                title=title[0:Length-words_after_Na-numLen-peoLen]+title[Length-words_after_Na:Length]
                Length=len(title)
                ws_results = ws([title])
                pos_results = pos(ws_results)
                ner_results = ner(ws_results, pos_results)
    #去除'團'與團前的字
    if('團' in title and (pos_results[0][-1]=='Na' and '團' in ws_results[0][-1])):
        tmpLen=len(ws_results[0][-1])
        deltaL=Length-tmpLen
        title=title[0:deltaL]
        Length=len(title)
        ws_results = ws([title])
        pos_results = pos(ws_results)
        ner_results = ner(ws_results, pos_results)
    #用'-'前的三個Na來判斷
    count=0
    true=0
    for j in pos_results[0][::-1]:
        if(j=='Na' and true<3):
            Na=ws_results[0][::-1][count]+" "+Na
            only_Na.append(ws_results[0][::-1][count])
            true=true+1
        count=count+1   
    #取外文
    if('FW' in pos_results[0]):
        for i in range(len(pos_results[0])-1,-1,-1):
            if(pos_results[0][i]=='FW' and ws_results[0][i]!='/' and ws_results[0][i]!='-' and ws_results[0][i] not in Na):
                Na=ws_results[0][i]+" "+Na
                f=ws_results[0][i].replace(' ','')
                if f=='office':
                    foreign_word.append(f+'365')
                elif (f=='netflix' or f=='netfilx' or f=='spotify' or f=='switch') and ('會員' in title or '家庭' in title):
                    foreign_word.append(f+' 會員')
                else:
                    foreign_word.append(f)
    #什麼都沒有
    if(len(Na)==0):
        for i in ws_results[0]:
            Na=Na+" "+i
    print(Na)
    #斷詞後全部結果拿去搜尋
    end=cluster(Na, item,Org_and_Product)
    #加上item_type
    if(end==1 and item_type in valid_type):
        tmp_Na=item_type+" "+Na
        end=cluster(tmp_Na,item,Org_and_Product)
    #用Product
    if(end==1 and ('PRODUCT' in Org_and_Product.keys())):
        end=cluster(Org_and_Product['PRODUCT'],item,Org_and_Product)
    #用ORG
    if(end==1 and ('ORG' in Org_and_Product.keys())):
        end=cluster(Org_and_Product['ORG'],item,Org_and_Product)
    #youtube,netflix,spotify這種特例
    if(end==1): 
        for i in special_fw:
            if(i in foreign_word):
                end=cluster(i,item,Org_and_Product)
    #一個一個刪減Na
    if(end==1 and item_type in valid_type):
        tmp_Na=""
        #用全部Na
        for i in only_Na:
            tmp_Na=i+" "+tmp_Na
        tmp_Na=item_type+" "+tmp_Na
        end=cluster(tmp_Na,item,Org_and_Product)
        #有三個Na
        if(end==1 and len(only_Na)==3):
            #用後兩個Na
            tmp_Na=item_type+" "+only_Na[1]+" "+only_Na[0]
            end=cluster(tmp_Na,item,Org_and_Product)
            #用前兩個Na
            if(end==1):
                tmp_Na=item_type+" "+only_Na[2]+" "+only_Na[1]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用第一個Na+第三個Na
            if(end==1): 
                tmp_Na=item_type+" "+only_Na[2]+" "+only_Na[0]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用最後一個Na
            if(end==1): 
                tmp_Na=item_type+" "+only_Na[0]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用第二個Na
            if(end==1):  
                tmp_Na=item_type+" "+only_Na[1]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用第一個Na
            if(end==1):   
                tmp_Na=item_type+" "+only_Na[2]
                end=cluster(tmp_Na,item,Org_and_Product)
        #只有兩個Na
        elif(end==1 and len(only_Na)==2):
            #用最後一個Na
            tmp_Na=item_type+" "+only_Na[0]
            end=cluster(tmp_Na,item,Org_and_Product)
            #用另一個Na
            if(end==1):
                tmp_Na=item_type+" "+only_Na[1]
                end=cluster(tmp_Na,item,Org_and_Product)
    #沒有加item_type
    if(end==1): 
        tmp_Na=""
        #用全部Na
        for i in only_Na:
            tmp_Na=i+" "+tmp_Na
        end=cluster(tmp_Na,item,Org_and_Product)
        #有三個Na
        if(end==1 and len(only_Na)==3):
            #用後兩個Na
            tmp_Na=only_Na[1]+" "+only_Na[0]
            end=cluster(tmp_Na,item,Org_and_Product)
            #用前兩個Na
            if(end==1):
                tmp_Na=only_Na[2]+" "+only_Na[1]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用第一個Na+第三個Na
            if(end==1):
                tmp_Na=only_Na[2]+" "+only_Na[0]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用最後一個Na
            if(end==1):
                tmp_Na=only_Na[0]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用第二個Na
            if(end==1):  
                tmp_Na=only_Na[1]
                end=cluster(tmp_Na,item,Org_and_Product)
            #用第一個Na
            if(end==1):  
                tmp_Na=only_Na[2]
                end=cluster(tmp_Na,item,Org_and_Product)
        #只有兩個Na
        elif(end==1 and len(only_Na)==2):
            #用最後一個Na
            tmp_Na=only_Na[0]
            end=cluster(tmp_Na,item,Org_and_Product)
            #用另一個Na
            if(end==1):
                tmp_Na=only_Na[1]
                end=cluster(tmp_Na,item,Org_and_Product)
    #用FW(英文)
    if(end==1 and len(foreign_word)>=1):
        end=cluster(foreign_word[0],item,Org_and_Product)
        if(end==1 and len(foreign_word)>1):
            end=cluster(foreign_word[0]+foreign_word[1],item,Org_and_Product)

#將PTT團購標題、回帖人數、發文時間爬取下來
end=1
while(end):
    for each in mainPage("#app > div > main > div  div.mt-2 > div:nth-child(n+1) > div").items():
        item={}
        title=each("div.e7-right.ml-2 > div.e7-right-top-container.e7-no-outline-all-descendants > a > span.e7-title.my-1.e7-link-to-article.e7-article-default > span:nth-child(1)").text()
        if each("div.e7-right.ml-2 > div.e7-right-top-container.e7-no-outline-all-descendants > a > span:nth-child(2) > span > span").text():
            continue
        else:
            if('[公告]' not in title and '[BuyTogether]' not in title and '[心得]' not in title and '[黑名單]' not in title and '[其他]' not in title and 'Re' not in title and '[灰人]' not in title and '[黑人]' not in title and '刪文' not in title and '刪除' not in title):
                time=each("div > div.e7-right.ml-2 > div.e7-meta-container > div.e7-grey-text > span:nth-child(2)").text()
                join=each("div > div.e7-left.e7-left-not-xs > div.e7-recommendCount.text-no-wrap.e7-grey-text").text()
                if(join==""):
                    join="0"
                if('[' in title and len(title)>4):    
                    item["type"]=title[(title.find('[')+1):title.find(']')]
                    item["title"]=title[4:]
                    time=time[0:time.rfind(' ')]
                    time=datetime.strptime(time, "%Y/%m/%d")
                    item["time"]=time
                    item["people"]=int(join.replace("回應:",""))
                    List.append(item)
                if('[' not in title):
                    item["type"]='綜合'
                    item["title"]=title
                    time=time[0:time.rfind(' ')]
                    time=datetime.strptime(time, "%Y/%m/%d")
                    item["time"]=time
                    item["people"]=int(join.replace("回應:",""))
                    List.append(item)

    mainPage.make_links_absolute(base_url=res.url) 
    if(num==104550):
        break
    else:
        num=num+25
        res = requests.get("https://www.pttweb.cc/bbs/BuyTogether/page?n="+str(num))
        mainPage = pq(res.text)

#將爬取下來的資料進行斷詞並分類
counter=0
for i in List:
    counter+=1
    ckip(i['title'],i['type'],i)