'''
目的：用granger causality驗證找出的leadership index對總團購量有顯著影響(有預測能力)
'''
import mysql.connector
import networkx as nx
import numpy as np
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import adfuller
from scipy import stats
from statsmodels.tsa.api import VAR
from statsmodels.tools.eval_measures import rmse, aic
import pickle
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import grangercausalitytests
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
mycursor4 = connection.cursor(buffered=True)

def add_months(sourcedate, weeks): 
    if(weeks==0):
        return sourcedate
    else:
        new=sourcedate-timedelta(days=7*(weeks))
        return new

d = datetime(2020, 7, 27) 
all_pr_time_series={}
thres_range=np.arange(0.05, 0.45, 0.01)
for i in thres_range:
    all_pr_time_series[i]=[] 
    for time in range1,9,1): 
        tmp_d=add_months(d,time) #當下時間點
        leaders_PR_sum=0  #加權值的分母
        time_point_amount=0 #在這個時間點的加權後團購量
        cur_PR_score=list() #目前時間點該分類的PR (加權值分子)
        cat_order=list()
        mycursor.execute("select * from Leaders_v2_ws12 where threshold>"+str(i-0.00001)+" and threshold<"+str(i+0.01-0.00001)+" and endTimePoint="+str(time)+";")
        for j in mycursor:
            mycursor1.execute("select * from category where id="+str(j[3])+";")
            for k in mycursor1:
                cat_order.append([k[1],k[3]])
                mycursor2.execute("select score from PageRank_v3_ws12 where fir_catid="+str(k[1])+" and sec_catid="+str(k[3])+" and thr_catid is NULL and windowSize=12 and endTime='"+(tmp_d).strftime("%Y-%m-%d")+" 00:00:00' and threshold>"+str(i-0.00001)+" and threshold<"+str(i+0.01-0.00001)+";")
                for p in mycursor2:
                    leaders_PR_sum+=p[0]
                    cur_PR_score.append(p[0])

        for g in cat_order: #[[fir_catid,sec_catid],[]]
            cur_amount=0
            mycursor3.execute("select * from timeSeries_week_new where fir_catid="+str(g[0])+" and sec_catid="+str(g[1])+" and thr_catid is NULL and year="+str(tmp_d.year)+" and month="+str(tmp_d.month)+" and week="+str(tmp_d.day)+";")
            for q in mycursor3:
                cur_amount=q[10]
            time_point_amount+=cur_amount*(cur_PR_score[cat_order.index(g)]/leaders_PR_sum)
        all_pr_time_series[i].append(time_point_amount)

# granger causality
start=0.15 # 起始threshold值
# 測試在不同threshold下的情況
for mul in range(30):
    i=start+(0.01*mul)
    size=9
    leader_list=all_pr_time_series[i][len(all_pr_time_series[i])-size:]
    sum_list=total_sum_list[len(total_sum_list)-size:]
    index_list=[]
    index_list.append(add_months(d,0))
    for j in range(1,size,1):
        tmp_d=add_months(d,-(j))
        index_list.append(tmp_d)
    data = pd.DataFrame(list(zip(leader_list, sum_list)), index =index_list ,columns =['leader', 'sum']) 
    rawData = data.copy(deep=True)

    # adf test
    X1sta=0
    X2sta=0
    X1 = np.array(data['leader'])
    X1 = X1[~np.isnan(X1)]
    result = adfuller(X1)
    for key, value in result[4].items():
        if result[0]<value: #stationary
            X1sta=1
            break

    X2 = np.array(data['sum'])
    X2 = X2[~np.isnan(X2)]
    result = adfuller(X2)
    for key, value in result[4].items():
        if result[0]<value: #stationary
            X2sta=1
            break

    if X1sta==0:
        data['leader'] = data['leader'] - data['leader'].shift(1)
    if X2sta==0:
        data['sum'] = data['sum'] - data['sum'].shift(1)
    data = data.dropna()
    res = grangercausalitytests(data[['sum','leader']], maxlag=1)