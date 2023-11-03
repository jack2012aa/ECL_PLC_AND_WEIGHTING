'''
這隻程式由黃章昱更改自蔡經理的PLCW.py。主要是整理註解，精簡程式碼以及對參數處理的標準化。
This program is modified by Chang-Yu Huang from PLCW.py, written by Mr.Tsai.
The purpose of this modification is to add comments, refine codes and standardize the passing of parameters.
'''

import os, csv, pandas as pd, time, serial, ctypes, sys
import modbus_tk.defines as cst
import modbus_tk.modbus_tcp as modbus_tcp
from modbus_tk import modbus_rtu
from datetime import datetime
from os.path import dirname, join

LENGTH_OF_A_BATCH = 40 
'''Number of records read in a batch.'''

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except:
        return False

def get_time_info():
    '''
    提供需要寫入csv的時間資訊。以後要改格式可以從這裡改。
    Provide time information used in writing csv files.
    '''

    now = datetime.now()
    return [
        now.year,
        now.month,
        now.day,
        now.hour,
        now.minute,
        now.second,
        now.microsecond,
        int(now.microsecond / 1000)
    ]

LENGTH_OF_TIME_INFO = len(get_time_info())
NAME_OF_SCALES = [2, 3]
'''Modbus id of scales'''

def set_file(name_of_scales: list):
    '''
    新建當週的csv檔並加入表頭。會有三個csv檔。
    Create csv files named with week. There will have 3 files.
    :param name_of_scales:所有會記錄到的飼料秤的名稱. Names of scales.
    '''

    dt = datetime.now()        
    wk = dt.isocalendar()[1]     
    yr = dt.isocalendar()[0]

    '''
    Create files.
    _feed_weight_raw_data_log.csv: raw data
    _feed_weight_calculated_filter.csv: 開啟重量去除標準差分析
    _feed_weight_calculated.csv: average per minute
    _feed_weight_calculated_avg_hr.csv: average per half hour
    '''
    files = [
        os.path.join(os.path.dirname(__file__),str(yr)+"_"+ str(wk) +"_"+ "feed_weight_raw_data_log.csv"),
        os.path.join(os.path.dirname(__file__), str(yr)+"_"+ str(wk) + "_"+"feed_weight_calculated_filter.csv"),
        os.path.join(os.path.dirname(__file__),str(yr)+"_"+ str(wk) +"_"+ "feed_weight_calculated.csv"),
        os.path.join(os.path.dirname(__file__), str(yr)+"_"+str(wk)+"_"+"feed_weight_calculated_avg_hr.csv")
    ]

    '''
    Create headers that are inserted into files.
    '''
    header = ['year','month','day','hour','minute','second','millisecond']
    header.append(name_of_scales)

    for file in files:
        with open(file, 'a', newline = '') as output:
          writer = csv.writer(output,delimiter=',',lineterminator='\n')

def read_data(master: modbus_tcp.TcpMaster, scales: list):
    '''
    磅秤讀值
    Read data from scales using Modbus TCP.
    :param master: TCP主機. Modbus TCP master.
    :param scales: 磅秤的modbus編號. Modbus ids of scales.
    '''

    now = datetime.now()
    '''A record that will be inserted into the csv file.'''
    record = get_time_info()

    '''Read from each scale.'''
    for scale in scales:
        weight = master.execute(scale, cst.READ_HOLDING_REGISTERS,0,2)
        '''
        判定數值是否為負值，以45000為臨界值。大於45000即為負值。
        因為錶頭顯示進位關係，可呈現最大值為65536，負值即為65536-X。
        EX、-200會顯示為65336。磅秤設定為小數點兩位，量測範圍為-200.00公斤
        至300.00公斤。顯示值應為0-30000/65336-65536，取45000為臨界值，確保#上下限值都不會在臨界值附近，造成誤判。
        This is something I can not understand.
        Generally, a weight that is bigger than 45,000 is actually a negative value.
        Here is to handle such situation.
        '''
        if weight > 45000:
            weight = weight - 65536
        '''g to kg'''
        weight = weight / 100
        record.append(weight)

    '''Write into a file.'''
    output_file = os.path.join(os.path.dirname(__file__),str(now.year)+"_"+ str(now.isocalendar()[1]) +"_"+ "feed_weight_raw_data_log.csv")
    with open(output_file,'a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        writer.writerow(record)
    
    print(record)

def weight_filter():
    '''
    為了降低豬對飼料秤晃動造成的影響，過濾超過上下限的數值，上下限為正負一個標準差，超過上下限的改為上下限值
    To minimize the effect of pigs on records, group every length_of_a_batch records and calibrate values out of one standard deviation.
    '''
    
    now = datetime.now()
    raw_data = pd.read_csv(join(dirname(__file__), str(now.year) + "_" + str(now.isocalendar()[1] + '_' + 'feed_weight_raw_data_log.csv')))[-LENGTH_OF_A_BATCH:]
    std = raw_data.std(axis = 0)
    average = raw_data.mean()

    for i in range(LENGTH_OF_A_BATCH):
        n_of_column = raw_data.shape[1]
        for j in range(LENGTH_OF_TIME_INFO,n_of_column):
            raw_data.iat[i,j] = min(raw_data.iat[i,j],average[j] + std[j])
            raw_data.iat[i,j] = max(raw_data.iat[i,j],average[j] - std[j])

    now = datetime.now()
    output_file = os.path.join(os.path.dirname(__file__),str(now.year) + '_' + str(now.isocalendar()[1] + '_' + 'feed_weight_calculated_filter.csv'))
    raw_data.to_csv(output_file,index = False)

    print(raw_data)

def calculate_filtered_average(input_file_name: str):
    '''
    計算篩選後的一批資料的平均值
    Calculate the average of filtered data.
    '''

    dataframe = pd.read_csv(input_file_name)[-LENGTH_OF_A_BATCH:]

    '''Insert time.'''
    average_data = []
    for i in range(LENGTH_OF_TIME_INFO):
        average_data.append(dataframe.iat[0,i])
    
    '''Insert average.'''
    mean = dataframe.mean()
    for i in range(LENGTH_OF_TIME_INFO,dataframe.shape[1]):
        average_data.append(mean[i])

    '''Write into csv.'''
    output_file = os.path.join(os.path.dirname(__file__), str(datetime.now().year)+"_"+str(datetime.now().isocalendar()[1])+"_"+"feed_weight_calculated.csv")
    with open(output_file,'a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        writer.writerow(average_data)
        
    print(average_data)

if is_admin():

    # 使用Modbus TCP
    #master_w = modbus_tcp.TcpMaster("192.168.100.94",6002)

    # 使用Modbus RTU
    master_w = modbus_rtu.RtuMaster(serial.Serial(port="com2", baudrate=38400, bytesize=8, parity='N', stopbits=1))
    master_w.set_timeout(5.0)
    master_w.set_verbose(True)

    while True:

        day = datetime.now().isocalendar()[2]
        time_hm = datetime.now().strftime("%H,%M")
        
        '''
        判定如果為當週第一天00：00時，執行子程式一，設定CSV檔及寫入錶頭
        Create new csv files.
        '''
        if time_hm =='00,00' and day==1:
            set_file()

        for i in range(40):
            read_data(master_w,NAME_OF_SCALES)
            time.sleep(0.2)
        weight_filter()
        calculate_filtered_average()
        time.sleep(60)
else:
    ctypes.windll.shell32.ShellExecuteW(None, "runas", sys.executable, __file__, None, 1)