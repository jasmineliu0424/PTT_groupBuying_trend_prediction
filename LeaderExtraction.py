'''
目的：依據PageRank score大小依序移除被目前這條時間序列領導的所有時間序列，找出所有能對其他時間序列帶來顯著影響的領導序列們
'''
import mysql.connector
import networkx as nx
import numpy as np
from datetime import datetime, timedelta

connection = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="passward",
  database="DB_name"
)

mycursor = connection.cursor(buffered=True)
mycursor1 = connection.cursor(buffered=True)
mycursor2 = connection.cursor(buffered=True)
mycursor3 = connection.cursor(buffered=True)

def add_months(sourcedate, weeks): 
    if(weeks==0):
        return sourcedate
    else:
        new=sourcedate-timedelta(days=7*(weeks))
        return new

# remove descendant time sereis(be led time series) 
def removedes(order,graph,lead_id):
    for cur_ts in order:
        if(order.index(cur_ts)>order.index(lead_id)): #在lead_id後面順序的時間序列
            if lead_id in graph[cur_ts].keys():
                removedes(order,graph,cur_ts)
                order.remove(cur_ts)

d = datetime(2020, 7, 27) 
leader_order={}
thres_range=np.arange(0.05, 0.45, 0.01) #threshold range
for threshold in thres_range:
    leader_order[threshold]=[]
    for t in range(1,9,1):  #往前9個時間點
        tmp_d=add_months(d,t)
        DG = nx.DiGraph() #B->A, A is leader
        mycursor1.execute("select * from AgCorrelation_week_ws12 where windowSize=12 and fir_catid2 is not NULL and fir_catid3 is NULL and agCorrelation>"+str(threshold)+" and endTime BETWEEN '"+(tmp_d-timedelta(days=1)).strftime("%Y-%m-%d")+" 12:00:00' AND '"+tmp_d.strftime("%Y-%m-%d")+" 23:30:00';")
        for i in mycursor1:
            beLed=0
            lead=0
            mycursor2.execute("select * from category where (fir_catid="+str(i[19])+" and sec_catid="+str(i[20])+") and thr_catid is NULL or (fir_catid="+str(i[16])+" and sec_catid="+str(i[17])+") and thr_catid is NULL;")
            for j in mycursor2:
                if j[1]==i[19] and j[3]==i[20]:
                    beLed=j[0]
                elif j[1]==i[16] and j[3]==i[17]:
                    lead=j[0]
            DG.add_weighted_edges_from([(beLed,lead,i[13])])

        #計算該有向權重圖的PageRank score
        pr=nx.pagerank(DG)
        for p in pr:
            mycursor.execute("select * from category where id="+str(p)+";")
            for c in mycursor:
                mycursor3.execute("insert into PageRank_v3_ws12 (fir_catid,sec_catid,thr_catid,endTime,week,score,threshold,windowSize) values ("+str(c[1])+","+str(c[3])+",NULL,'"+str(tmp_d)+"','"+str(t)+"','"+str(pr[p])+"',"+str(threshold)+",12);")
                connection.commit()
        result = {k: v for k, v in sorted(pr.items(), key=lambda item: item[1], reverse=True)}
        order=[]
        for i in result:
            order.append(i)
        
        #根據PageRank score大小依序移除
        for id in order:
            removedes(order,DG,id)
        leader_order[threshold].append(order)
        
thres_range=np.arange(0.05, 0.45, 0.01)
for i in thres_range: #0.15 ~ 0.45
    timePoint=0
    for j in leader_order[i]: 
        for k in j: #leaders
            mycursor3.execute("insert into Leaders_v2_ws12 (threshold,endTimePoint,leaderID) values ("+str(i)+","+str(timePoint)+","+str(k)+");")
            connection.commit()
        timePoint=timePoint-1