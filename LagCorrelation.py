'''
目的：計算倆倆時間序列之間的correlation，並找出其中的領導關係
'''
from datetime import datetime, timedelta
import mysql.connector
import statistics
import numpy as np

connection = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="passward",
  database="DB_name"
)

mycursor = connection.cursor(buffered=True)

def add_months(sourcedate, weeks): 
    if(weeks==0):
        return sourcedate
    else:
        new=sourcedate-timedelta(days=7*(weeks))
        return new

# 計算兩條時間序列在特定lag、特定window下的相關程度
def correlation(fir_timeSerie, sec_timeSerie, time, lag, window):
    #擷取mean和standard deviation要計算的段落
    new_fir=[]
    new_sec=[]
    fir_start_time=add_months(time,(window-1-lag))
    sec_end_time=add_months(time,lag)
    sec_start_time=add_months(time,(window-1))
    for i in fir_timeSerie:
        if(i['time']>=fir_start_time and i['time']<=time):
            new_fir.append(i['number'])
    for j in sec_timeSerie:
        if(j['time']>=sec_start_time and j['time']<=sec_end_time):
            new_sec.append(j['number'])
    #計算mean和standard deviation
    mean_f=statistics.mean(new_fir)
    mean_s=statistics.mean(new_sec)
    stdev_f=statistics.stdev(new_fir)
    stdev_s=statistics.stdev(new_sec)

    #計算公式分子部分
    sum=0
    for i, j in zip(new_fir, new_sec):
        sum+=(i-mean_f)*(j-mean_s)
    sum=sum/(window-1)
    if(stdev_f*stdev_s):
        correlation=sum/(stdev_f*stdev_s)
    else:
        correlation=0
    return correlation

# 計算兩條時間序列間的aggregated correlation，得出lead-lag relation
def aggregated_correlation(fir_timeSerie, sec_timeSerie, time, window):
    #計算出所有lag的correlation value
    c=[] #所有lag的correlation
    c_pos=[] #lag>=0的correlation
    pos_sum=0
    c_neg=[] #lag<0的correlation
    neg_sum=0

    for i in range(-(window//2),(window//2)+1,1):
        if i<0:
            n=correlation(sec_timeSerie, fir_timeSerie, time, abs(i), window)
            c_neg.append(n)
        else:
            p=correlation(fir_timeSerie, sec_timeSerie, time, i, window)
            c_pos.append(p)

    for i, j in zip(c_pos, c_neg):
        pos_sum+=max(i,0)/((window//2)+1)
        neg_sum+=max(j,0)/(window//2)
    if(pos_sum>=neg_sum): #sec lead fir
        aggregate_cor=pos_sum
        return [aggregate_cor,sec_timeSerie[0]['catid'],fir_timeSerie[0]['catid']]
    else: #fir lead sec
        aggregate_cor=neg_sum
        return [aggregate_cor,fir_timeSerie[0]['catid'],sec_timeSerie[0]['catid']]

mycursor.execute("select fir_catid,fir_category from category where sec_catid is  NULL and thr_catid is NULL;")
fir_cat=[]
for j in mycursor:
    fir_cat.append([j[0],j[1]]) 

d = datetime(2020, 7, 27) #從哪個時間點開始往前
for k in range(1,9,1): #從時間點d往前9個時間點(週為單位)
    tmp_d=add_months(d, k)
    range_d=add_months(tmp_d,7)
    for f in fir_cat: 
        fir=[]
        mycursor.execute("select * from timeSeries_week where fir_catid="+str(f[0])+" and sec_catid is NULL and year>="+str(range_d.year)+";")
        for i in mycursor:
            time=datetime(i[7], i[8], i[9])
            if time>tmp_d:
                break
            elif time<range_d:
                continue
            else:
                fir.append({'catid':i[1],'cat':i[2],'time':time,'number':i[10]})
        for s in fir_cat[fir_cat.index(f)+1:]:
            sec=[]
            mycursor.execute("select * from timeSeries_week where fir_catid="+str(s[0])+" and sec_catid is NULL and year>="+str(range_d.year)+";")
            for i in mycursor:
                time=datetime(i[7], i[8], i[9])
                if time>tmp_d:
                    break
                elif time<range_d:
                    continue
                else:
                    sec.append({'catid':i[1],'cat':i[2],'time':time,'number':i[10]})
            ag=aggregated_correlation(fir,sec,tmp_d,8) #list[cor,leaderID]
            mycursor.execute("insert into AgCorrelation_week_v2 (fir_catid1,fir_category1,fir_catid2,fir_category2,fir_catid3,fir_category3,fir_catid1,fir_category1,fir_catid2,fir_category2,fir_catid3,fir_category3,agCorrelation,endTime,windowSize,fir_leaderID,sec_leaderID,thr_leaderID,fir_beLedID,sec_beLedID,thr_beLedID) values ("+str(fir[0]['catid'])+","+"'"+fir[0]['cat']+"',NULL,NULL,NULL,NULL,"+str(sec[0]['catid'])+","+"'"+sec[0]['cat']+"',NULL,NULL,NULL,NULL,"+str(ag[0])+","+"'"+str(tmp_d)+"'"+",8,"+str(ag[1])+",NULL,NULL,"+str(ag[2])+",NULL,NULL);")
            connection.commit()