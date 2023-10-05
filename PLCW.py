# -*- coding: utf_8 -*-
#導入相關應用函式資料庫
import modbus_tk.defines as cst
import modbus_tk.modbus_tcp as modbus_tcp
import time
import csv
from datetime import datetime
import os
import pandas as pd
from os.path import dirname, join

#連接MODBUS TCP從機
master_w = modbus_tcp.TcpMaster("192.168.100.94",6002)
master_w.set_timeout(5.0)
master_w.set_verbose(True)
##設定時間格式年,月,日,時,分,秒
time_data=time.strftime('%Y,%m,%d,%I,%M,%S')

##子程式一、每週第一天需開啟一CSV檔，並輸入檔案表頭。
## CSV檔有三個: 1.raw data, 2.filtered 3.minute-averaged 4.half-hour-averaged 
def set_file():

    #宣告時間相關參數
    dt = datetime.now()        ##現在時間
    wk = dt.isocalendar()[1]     ##本週第X天
    yr = dt.isocalendar()[0]     ##現在是西元X年
    
    #設定開啟檔案，路與程式同資料夾，
    outputFilePath2 = os.path.join(os.path.dirname(__file__),
                 str(yr)+"_"+ str(wk) +"_"+ "feed_weight_raw_data_log.csv")
        
   #開啟檔案
    with open(outputFilePath2,'a',newline='' ) as output: 
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
    #寫入表頭內容      writer.writerow(['year','month','day','hour','minute','second' ,'millisecond','weightl1','weightl2','weightl3','weightl4','weightl5','weightl6','weighth1','weighth2','weighth3','weighth4','weighth5','weighth6']) 

    #開啟重量去除標準差分析記錄檔，設定路徑、檔名，寫入表頭
    outputFilePath4 = os.path.join(os.path.dirname(__file__), str(yr)+"_"+ str(wk) + "_"+"feed_weight_calculated_fillter.csv")
       
    with open(outputFilePath4,'a',newline='' ) as output: 
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
          writer.writerow(['year','month','day','hour','minute','second','millisecond','weightl1','weightl2','weightl3','weightl4','weightl5','weightl6'
                           ,'weighth1','weighth2','weighth3','weighth4','weighth5','weighth6']) 
          
    #開啟重量分鐘平均分析記錄檔，設定路徑、檔名，寫入表頭
    outputFilePath5 = os.path.join(os.path.dirname(__file__),str(yr)+"_"+ str(wk) +"_"+ "feed_weight_calculated.csv")
       
    with open(outputFilePath5,'a',newline='' ) as output: 
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
          writer.writerow(['year','month','day','hour','minute','second','millisecond','weightl1','weightl2','weightl3','weightl4','weightl5','weightl6','weighth1','weighth2','weighth3','weighth4','weighth5','weighth6']) 

    #開啟重量半小時平均分析記錄檔，設定路徑、檔名，寫入表頭

    outputFilePath9 = os.path.join(os.path.dirname(__file__), str(yr)+"_"+str(wk)+"_"+"feed_weight_calculated_avg_hr.csv")

    with open(outputFilePath9,'a',newline='' ) as output:
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
          writer.writerow(['year','month','day','hour','minute','second','millisecond','weightl1','weightl2','weightl3','weightl4','weightl5','weightl6','weighth1','weighth2','weighth3','weighth4','weighth5','weighth6'])

#子程式一、磅秤讀值RAW_DATA記錄
def weight_data():
    
  dt = datetime.now() 
  wk = dt.isocalendar()[1]
 
  #讀取磅秤錶頭暫存器內數值，2-7為矮側，12-17為高側
  wl1 = master_w.execute(2, cst.READ_HOLDING_REGISTERS,0,2)
  wl2 = master_w.execute(3, cst.READ_HOLDING_REGISTERS,0,2)
  wl3 = master_w.execute(4, cst.READ_HOLDING_REGISTERS,0,2)
  wl4 = master_w.execute(5, cst.READ_HOLDING_REGISTERS,0,2)
  wl5 = master_w.execute(6, cst.READ_HOLDING_REGISTERS,0,2)
  wl6 = master_w.execute(7, cst.READ_HOLDING_REGISTERS,0,2)
  wh1 = master_w.execute(12, cst.READ_HOLDING_REGISTERS,0,2)
  wh2 = master_w.execute(13, cst.READ_HOLDING_REGISTERS,0,2)
  wh3 = master_w.execute(14, cst.READ_HOLDING_REGISTERS,0,2)
  wh4 = master_w.execute(15, cst.READ_HOLDING_REGISTERS,0,2)
  wh5 = master_w.execute(16, cst.READ_HOLDING_REGISTERS,0,2)
  wh6 = master_w.execute(17, cst.READ_HOLDING_REGISTERS,0,2)
#設定時間參數
  y=datetime.now().year            #y=年
  m=datetime.now().month         #m=月
  d=datetime.now().day            #d=日
  h=datetime.now().hour          #h=時
  mm=datetime.now().minute     #mm=分
  s=datetime.now().second       #s=秒
  ms=datetime.now().microsecond 
 #時間格式無毫秒，用microsecond/1000取得毫秒值
  ss=int(ms/1000)              #ss=毫秒


#定義寫入CSV檔資料內容及排序
  df_wd = [y,m,d,h,mm,s,ss,int(wl1[0]),int(wl2[0]),int(wl3[0]),int(wl4[0]),int(wl5[0]),int(wl6[0])
          ,int(wh1[0]),int(wh2[0]),int(wh3[0]),int(wh4[0]),int(wh5[0]),int(wh6[0])]
  


#判定數值是否為負值，以45000為臨界值。大於45000即為負值。
#因為錶頭顯示進位關係，可呈現最大值為65536，負值即為65536-X。
#EX、-200會顯示為65336。磅秤設定為小數點兩位，量測範圍為-200.00公斤
#至300.00公斤。顯示值應為0-30000/65336-65536，取45000為臨界值，確保#上下限值都不會在臨界值附近，造成誤判。
  while True:
     n=0
    
     if df_wd[7]>45000:
         l1=df_wd[7]-65536
         df_wd[7]=l1
    
     if df_wd[8]>45000:
         l2=df_wd[8]-65536
         df_wd[8]=l2
    
     if df_wd[9]>45000:
         l3=df_wd[9]-65536
         df_wd[9]=l3
    
     if df_wd[10]>45000:
         l4=df_wd[10]-65536
         df_wd[10]=l4
     
     if df_wd[11]>45000:
         l5=df_wd[11]-65536
         df_wd[11]=l5
     
     if df_wd[12]>45000:
         l6=df_wd[12]-65536
         df_wd[12]=l6
     
     if df_wd[13]>45000:
         h1=df_wd[13]-65536
         df_wd[13]=h1
     
     if df_wd[14]>45000:
         h2=df_wd[14]-65536
         df_wd[14]=h2
     
     if df_wd[15]>45000:
         h3=df_wd[15]-65536
         df_wd[15]=h3
     
     if df_wd[16]>45000:
         h4=df_wd[16]-65536
         df_wd[16]=h4
         
     if df_wd[17]>45000:
         h5=df_wd[17]-65536
         df_wd[17]=h5
          
     if df_wd[18]>45000:
         h6=df_wd[18]-65536
         df_wd[18]=h6
            
         
     n=n+1
     if n==1:
         break
  else: 
          pass

  #定義寫入csv檔內容及排序，錶頭設定為小數點兩位，因此數值都需/100 
  weight_rawdata = [y,m,d,h,mm,s,ss,df_wd[7]/100,df_wd[8]/100,df_wd[9]/100,df_wd[10]/100                   ,df_wd[11]/100,df_wd[12]/100,df_wd[13]/100,df_wd[14]/100                   ,df_wd[15]/100,df_wd[16]/100,df_wd[17]/100,df_wd[18]/100] 
    

##開啟檔案，寫入內容
  outputFilePath2 = os.path.join(os.path.dirname(__file__),
             str(yr)+"_"+ str(wk) +"_"+ "feed_weight_raw_data_log.csv")

    
  with open(outputFilePath2,'a',newline='' ) as output: 
      writer = csv.writer(output,delimiter=',',lineterminator='\n')
      writer.writerow(weight_rawdata)
    
  ##顯示數據資料，確定程式碼有正確執行
  print(weight_rawdata) 
  
  
##子程式三、過濾數值
def weight_filter():
   

 df_weight_rawdata = pd.read_csv(join(dirname(__file__),
        str(yr)+"_"+ str(wk) +"_" + "feed_weight_raw_data_log.csv"))

#nw1為數據總筆數
 nw1=len(df_weight_rawdata)
#存取RAW_DATA最新的40筆數據
 df_weight_std = df_weight_rawdata[nw1-40:nw1].std(axis=0)
#40筆數據取平均值
 df_avg = df_weight_rawdata[nw1-40:nw1].mean()

#取得最新一筆數據值
 df_nw = df_weight_rawdata[nw1-1:nw1]


#設定上限臨界值，為平均值加一個標準差。
 stw_l1 =int(df_avg[7]+df_weight_std[7])
 stw_l2 =int(df_avg[8]+df_weight_std[8])
 stw_l3 =int(df_avg[9]+df_weight_std[9])
 stw_l4 =int(df_avg[10]+df_weight_std[10])
 stw_l5 =int(df_avg[11]+df_weight_std[11])
 stw_l6 =int(df_avg[12]+df_weight_std[12])
 stw_h1 =int(df_avg[13]+df_weight_std[13])
 stw_h2 =int(df_avg[14]+df_weight_std[14])
 stw_h3 =int(df_avg[15]+df_weight_std[15])
 stw_h4 =int(df_avg[16]+df_weight_std[16])
 stw_h5 =int(df_avg[17]+df_weight_std[17])
 stw_h6 =int(df_avg[18]+df_weight_std[18])
 
 
 #設定下限臨界值為平均值減一個標準差
 stw_l11 =int(df_avg[7]-df_weight_std[7])
 stw_l21 =int(df_avg[8]-df_weight_std[8])
 stw_l31 =int(df_avg[9]-df_weight_std[9])
 stw_l41 =int(df_avg[10]-df_weight_std[10])
 stw_l51 =int(df_avg[11]-df_weight_std[11])
 stw_l61 =int(df_avg[12]-df_weight_std[12])
 stw_h11 =int(df_avg[13]-df_weight_std[13])
 stw_h21 =int(df_avg[14]-df_weight_std[14])
 stw_h31 =int(df_avg[15]-df_weight_std[15])
 stw_h41 =int(df_avg[16]-df_weight_std[16])
 stw_h51 =int(df_avg[17]-df_weight_std[17])
 stw_h61 =int(df_avg[18]-df_weight_std[18])
 
 
 #進行資料篩選，大於上限值或小於下限值數值都以臨界值替代。
 while True:
     n=0
              
     if df_nw.iat[0,7] > stw_l1:
      df_nw.iat[0,7]=stw_l2
     elif df_nw.iat[0,7] <stw_l11:
      df_nw.iat[0,7]=stw_l11
     else:
         pass
         
     if df_nw.iat[0,8] > stw_l2:
      df_nw.iat[0,8]= stw_l2
     elif df_nw.iat[0,8] <stw_l21:
      df_nw.iat[0,8]=stw_l21
     else:
         pass
      
        
     if df_nw.iat[0,9] > stw_l3:
      df_nw.iat[0,9]=stw_l3
     elif df_nw.iat[0,9] <stw_l31:
      df_nw.iat[0,9]=stw_l31
     else:
         pass
    
      
     if df_nw.iat[0,10] > stw_l4:
      df_nw.iat[0,10]=stw_l4
     elif df_nw.iat[0,10] <stw_l41:
      df_nw.iat[0,10]=stw_l41
     else:
         pass
      
     if df_nw.iat[0,11] > stw_l5:
      df_nw.iat[0,11]=stw_l5
     elif df_nw.iat[0,11] <stw_l51:
      df_nw.iat[0,11]=stw_l51
     else:
         pass
    
     if df_nw.iat[0,12] > stw_l6:
      df_nw.iat[0,12]=stw_l6
     elif df_nw.iat[0,12] <stw_l61:
      df_nw.iat[0,12]=stw_l61
     else:
         pass
      
     if df_nw.iat[0,13] > stw_h1:
      df_nw.iat[0,13]=stw_h1
     elif df_nw.iat[0,13] <stw_h11:
      df_nw.iat[0,13]=stw_h11
     else:
         pass
      
     if df_nw.iat[0,14] > stw_h2:
      df_nw.iat[0,14]=stw_h2
     elif df_nw.iat[0,14] <stw_h21:
      df_nw.iat[0,14]=stw_h21
     else:
         pass
      
     if df_nw.iat[0,15] > stw_h3:
      df_nw.iat[0,15]=stw_h3
     elif df_nw.iat[0,15] <stw_h31:
      df_nw.iat[0,15]=stw_h31
     else:
         pass
      
     if df_nw.iat[0,16] > stw_h4:
      df_nw.iat[0,16]=stw_h4
     elif df_nw.iat[0,16] <stw_h41:
      df_nw.iat[0,16]=stw_h41
     else:
         pass
      
     if df_nw.iat[0,17] > stw_h5:
      df_nw.iat[0,17]=stw_h5
     elif df_nw.iat[0,17] <stw_h51:
      df_nw.iat[0,17]=stw_h51
     else:
         pass  
     
     if df_nw.iat[0,18] > stw_h6:
      df_nw.iat[0,18]=stw_h6
     elif df_nw.iat[0,18] <stw_h61:
      df_nw.iat[0,18]=stw_l11
     else:
         pass
     n=n+1
     if n==1:
         break
 else: 
     pass




#定義時間參數
 y=datetime.now().year
 m=datetime.now().month
 d=datetime.now().day
 h=datetime.now().hour
 mm=datetime.now().minute
 s=datetime.now().second
 ms=datetime.now().microsecond
 ss=int(ms/1000)
   
  

#定義寫入資料內容及排序
data_nw=[y,m,d,h,mm,s,ss,df_nw.iat[0,7],df_nw.iat[0,8],df_nw.iat[0,9],df_nw.iat[0,10],df_nw.iat[0,11],df_nw.iat[0,12],df_nw.iat[0,13],df_nw.iat[0,14],df_nw.iat[0,15],df_nw.iat[0,16],df_nw.iat[0,17], df_nw.iat[0,18]]
#開啟檔案，西元年_週次_feed_weight_calculated_fillter.csv
 outputFilePath4 = os.path.join(os.path.dirname(__file__)
                ,str(yr)+"_"+str(wk)+"_"+"feed_weight_calculated_fillter.csv")

 #寫入資料
 with open(outputFilePath4,'a',newline='' ) as output:
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
          writer.writerow(data_nw)

print(data_nw)











#子程式四、計算平均值
def weight_avg():
 
  wk = datetime.now().isocalendar()[1]

#載入去除標準差之數據資料
  df_weight_fill = pd.read_csv(join(dirname(__file__)
                 ,str(yr)+"_"+str(wk)+"_"+"feed_weight_calculated_fillter.csv"))

#計算資料長度
  nw2=len(df_weight_fill)
#取最新的40筆資料計算平均值
  df_avg_min = df_weight_fill[nw2-40:nw2].mean()
#定議時間資料參數
  y=datetime.now().year
  m=datetime.now().month
  d=datetime.now().day
  h=datetime.now().hour
  mm=datetime.now().minute
  s=datetime.now().second
  ms=datetime.now().microsecond
  ss=int(ms/1000)
   
#建立資料寫入內容及排序
  data_avg =[y,m,d,h,mm,s,ss,df_avg_min[7],df_avg_min[8],df_avg_min[9],df_avg_min[10],df_avg_min[11],df_avg_min[12],df_avg_min[13],df_avg_min[14],df_avg_min[15],df_avg_min[16],df_avg_min[17],df_avg_min[18]]
#設定存檔路徑及檔案名稱
  outputFilePath5 = os.path.join(os.path.dirname(__file__),                         str(yr)+"_"+str(wk)+"_"+"feed_weight_calculated.csv")
#寫入計算後平均值數據
  with open(outputFilePath5,'a',newline='' ) as output:
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
          writer.writerow(data_avg)
#印出數據確定是否執行程式
  print(data_avg)

#子程式五，計算15筆平均數值後加總平均
def avg_2():
  wk = datetime.now().isocalendar()[1]

#讀取檔案，西元年_週次_ feed_weight_calculated.csv
  df_weight_avg = pd.read_csv(join(dirname(__file__)
                    ,str(yr)+"_"+str(wk)+"_"+"feed_weight_calculated.csv"))
#計算數據資料筆數
  nw3=len(df_weight_avg)
#計算最新15筆數據平均值
  df_avg_hr = df_weight_avg[nw3-15:nw3].mean()
  
#設定時間單位參數
  y=datetime.now().year
  m=datetime.now().month
  d=datetime.now().day
  h=datetime.now().hour
  mm=datetime.now().minute
  s=datetime.now().second
  ms=datetime.now().microsecond
  ss=int(ms/1000)
   
  
#定義寫入資訊內容及排序
  data_avg2 =[y,m,d,h,mm,s,ss,df_avg_hr[7],df_avg_hr[8],df_avg_hr[9],df_avg_hr[10],df_avg_hr[11],df_avg_hr[12],df_avg_hr[13],df_avg_hr[14],df_avg_hr[15],df_avg_hr[16],df_avg_hr[17],df_avg_hr[18]]
#開啟CSV檔
  outputFilePath9 = os.path.join(os.path.dirname(__file__),                                 str(yr)+"_"+str(wk)+"_"+"feed_weight_calculated_avg_hr.csv")
#寫入資料
  with open(outputFilePath9,'a',newline='' ) as output:
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
          writer.writerow(data_avg2)
 #印出半小時平均資料，確認程式執行
  print('avg_in_hr:')
  print(data_avg2)

    
        
#設定時間日期相關參數
dt = datetime.now()
    
day = dt.isocalendar()[2]

mm =int( dt.strftime('%M'))

nh=0
var = 1
while var == 1:

    day = datetime.now().isocalendar()[2]
    
    time_hm = datetime.now().strftime("%H,%M")
#判定如果為當週第一天00：00時，執行子程式一，設定CSV檔及寫入錶頭
    if time_hm =='00,00' and day==1:
        print('new week')
        set_file()
        n=0                                      #計數器歸零
        while True:
            weight_data()                          #執行raw_data寫入 
            n = n+1                               #計數器+1
            print('weight_data log count-new',n)
            time.sleep(0.2)                         #寫入間隔0.2秒
            if n==40:                              #寫入40筆後離開迴圈
                break
        time.sleep(60)                             #待機60秒
    else:
        
        nw=0                          #每2分鐘寫入40資料計數器歸零
        while True:
            weight_data()                            #寫入raw_data資料
            nw = nw+1                                       #計數器+1
            print('weight_data log count',nw)
            time.sleep(0.2)                              #寫入間隔0.2秒
            weight_filter()                #過慮大於或小於一個標準差的值
            if nw == 40:                       #計數器為40後計算平均值                               
               weight_avg()                  #執行子程式，40筆後取平均
               nh=nh+1            #計數器+1（每40筆數據取一次平均）
               print(nh)
               break
            else:
               pass
                 
            if nh==15:           #取15 平均值後，計算15個數據的平均值
               avg_2()                  #執行子程式，計算30分鐘平均值
               nh=0                                       #計數器歸零
               break                                        #離開迴圈
            else:
               pass
        time.sleep(60)                                       #待機60秒
else:
           pass
  
