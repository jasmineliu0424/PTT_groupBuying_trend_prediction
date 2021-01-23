'''
目的：將各類別依據各時間點的團購總人數形成一條條時間序列
'''
from datetime import datetime, timedelta
import mysql.connector

def add_months(sourcedate, weeks): 
    if(weeks==0):
        return sourcedate
    else:
        new=sourcedate-timedelta(days=7*(weeks))
        return new

connection = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="passward",
  database="DB_name"
)

mycursor = connection.cursor(buffered=True)

# 蝦皮第一層商品類別
mycursor.execute("select fir_catid,fir_category from category where sec_catid is NULL;")
main_cat=[]
for j in mycursor:
    main_cat.append([j[0],j[1]]) 

for j in main_cat:
    origin = datetime(2020, 7, 26) #從哪個時間點開始往前
    for i in range(1,9,1): #要幾個時間點
        if(i==1):
            d=origin+timedelta(days=1)
            new_d=str(d)
            new_d = datetime.strptime(new_d, '%Y-%m-%d %H:%M:%S')
            new_d = new_d.strftime('%Y-%m-%d')
        else:
            d=t+timedelta(days=1)
            new_d=str(d)
            new_d = datetime.strptime(new_d, '%Y-%m-%d %H:%M:%S')
            new_d = new_d.strftime('%Y-%m-%d')

        t=d + timedelta(days=6)
        n_t=str(t)
        n = datetime.strptime(n_t, '%Y-%m-%d %H:%M:%S')
        n = n.strftime('%Y-%m-%d')
        mycursor.execute("select * from product where time between "+ "'" + new_d +" 00:00:00' and "+"'"+ n + " 00:00:00' and fir_catid="+str(j[0])+";")
        sum=0
        for k in mycursor:
            sum+=int(k[3])
        mycursor.execute("insert into timeSeries_week (fir_catid,layerOne_category,sec_catid,layerTwo_category,thr_catid,layerThr_category,year,month,week,amount) values ("+str(j[0])+","+"'"+j[1]+"'"+",NULL,NULL,NULL,NULL,"+str(d.year)+","+str(d.month)+","+str(d.day)+","+str(sum)+");")
        connection.commit()
        
