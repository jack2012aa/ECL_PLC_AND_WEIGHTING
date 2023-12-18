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
DURATION = 0.2
'''Duration between two records in a batch.'''
TOS = 9
'''Time of sleeping'''

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

def generate_file_name():
    '''
    生成這週用的檔案名 \n
    Generate the name of files used in this week. \n
    檔案列表 list:
    * raw_data.csv
    * std_filtered.csv
    * std_filtered_1min_average.csv
    '''

    dt = datetime.now()        
    wk = dt.isocalendar()[1]     
    yr = dt.isocalendar()[0]

    files = [os.path.join(os.path.dirname(__file__),str(yr)+"_"+ str(wk) +"_"+ "raw_data.csv"),
            os.path.join(os.path.dirname(__file__), str(yr)+"_"+ str(wk) + "_"+"std_filtered.csv"),
            os.path.join(os.path.dirname(__file__),str(yr)+"_"+ str(wk) +"_"+ "std_filtered_1min_average.csv"),
        ]
    return files


def exist_file():
    '''
    查看是否已經存在這週的檔案
    Check whether csv of this week is exist.
    '''''

    files = generate_file_name()

    result = True

    for file in files:
        result = result and os.path.isfile(file)

    return result


def set_file(name_of_scales: list):
    '''
    新建當週的csv檔並加入表頭。會有三個csv檔。
    Create csv files named with week. There will have 3 files.
    :param name_of_scales:所有會記錄到的飼料秤的名稱. Names of scales.
    '''

    files = generate_file_name()

    '''
    Create headers that are inserted into files.
    '''
    header = ['year','month','day','hour','minute','second','millisecond']
    for scale in name_of_scales:
        header.append(scale)

    for file in files:
        with open(file, 'a', newline = '') as output:
          writer = csv.writer(output,delimiter=',',lineterminator='\n')
          writer.writerow(header)

def read_data(master: modbus_tcp.TcpMaster, scales: list):
    '''
    磅秤讀值
    Read data from scales using Modbus TCP.
    :param master: TCP主機. Modbus TCP master.
    :param scales: 磅秤的modbus編號. Modbus ids of scales.
    '''

    '''A record that will be inserted into the csv file.'''
    record = get_time_info()

    '''Read from each scale.'''
    for scale in scales:
        weight = master.execute(scale, cst.READ_HOLDING_REGISTERS,0,2)[0]
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
    file = generate_file_name()[0]    
    with open(file,'a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        writer.writerow(record)
    
    print(record)

def std_filter():
    '''
    為了降低豬對飼料秤晃動造成的影響，過濾超過上下限的數值，上下限為正負一個標準差，超過上下限的改為上下限值
    To minimize the effect of pigs on records, group every length_of_a_batch records and calibrate values out of one standard devilocion.
    '''
    
    raw_data = pd.read_csv(generate_file_name()[0], on_bad_lines = 'skip')
    
    group = raw_data[-LENGTH_OF_A_BATCH:]
    group = group.astype(float)
    std = group.std()
    average = group.mean()

    with open(generate_file_name()[1],'a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        for i in range(LENGTH_OF_A_BATCH):
            for j in range(LENGTH_OF_TIME_INFO, raw_data.shape[1]):
                group.iloc[i,j] = min(group.iloc[i,j],average[j] + std[j])
                group.iloc[i,j] = max(group.iloc[i,j],average[j] - std[j])
            writer.writerow(group.iloc[i])

    print(group)

def one_min_average():
    '''
    計算篩選後的一批資料的平均值
    Calculate the average of filtered data.
    '''

    now = datetime.now()
    df = pd.read_csv(generate_file_name()[1], on_bad_lines = 'skip')[-LENGTH_OF_A_BATCH:]

    '''Insert time.'''
    average_data = []
    for i in range(LENGTH_OF_TIME_INFO):
        average_data.append(df.iloc[0,i])
    
    '''Insert average.'''
    mean = df.mean()
    for i in range(LENGTH_OF_TIME_INFO,df.shape[1]):
        average_data.append(mean[i])

    '''Write into csv.'''
    with open(generate_file_name()[2],'a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        writer.writerow(average_data)
        
    print(average_data)


# 使用Modbus TCP
#master_w = modbus_tcp.TcpMaster("192.168.100.94",6002)

# 使用Modbus RTU
master_w = modbus_rtu.RtuMaster(serial.Serial(port="com2", baudrate=38400, bytesize=8, parity='N', stopbits=1))
master_w.set_timeout(5.0)
master_w.set_verbose(True)

while True:

    '''
    如果這週的檔案還沒被建立，設定CSV檔及寫入錶頭
    Create new csv files.
    '''
    if not exist_file():
        set_file(NAME_OF_SCALES)

    for i in range(LENGTH_OF_A_BATCH):
        read_data(master_w,NAME_OF_SCALES)
        time.sleep(DURATION)
    std_filter()
    one_min_average()
    time.sleep(TOS)
