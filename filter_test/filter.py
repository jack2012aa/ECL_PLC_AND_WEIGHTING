import pandas as pd, csv
from datetime import datetime
from matplotlib import pyplot as plt


def std_filter():
    '''
    將40筆資料中超出一個標準差的數據替換成加減一個標準差
    '''
    
    raw_data = pd.read_csv('raw01.csv')
    i = 0
    with open('std_output.csv','a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        while i + 40 < raw_data.size / 10:
            group = raw_data[i:i+40]
            std = group.std()
            print(std)
            average = group.mean()
            for j in range(40):
                for k in [8,9]:
                    group.iloc[j,k] = min(group.iloc[j,k],average[k] + std[k])
                    group.iloc[j,k] = max(group.iloc[j,k],average[k] - std[k])
                writer.writerow(group.iloc[j])
                print(group.iloc[j])
            group = None
            i += 40

def MA_filter(n):
    '''
    n筆的移動平均
    '''

    sum = [0,0]
    last = 0
    raw_data = pd.read_csv('raw01.csv')
    with open('MA_' + str(n) + '_output.csv','a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        for i in range(int(raw_data.size / 10)):
            for j in range(2):
                sum[j] += raw_data.iat[i,8+j]
            if i > n:
                for j in range(2):
                    sum[j] -= raw_data.iat[last,8+j]
                last += 1
            writer.writerow([sum[0]/min(n,i+1),sum[1]/min(n,i+1)])
            print([sum[0]/min(n,i+1),sum[1]/min(n,i+1)])
            
def cal_std(n):
    '''
    計算從i往前n筆的標準差
    '''
    raw_data = pd.read_csv('raw01.csv')
    with open('std.csv','a',newline = '') as output:
        writer = csv.writer(output,delimiter=',',lineterminator='\n')
        i = n
        while i < raw_data.size / 10:
            group = raw_data[i-n:i]
            std = group.std().iloc[9]
            data = group.iloc[n-1].tolist()
            data.append(std)
            writer.writerow(data)
            i += 1


def draw_std(n,thred,exclude):
    '''
    計算從i往前n筆的標準差並繪圖，如果標準差大於s的話使用不同顏色標註
    '''

    raw_data = pd.read_csv('raw01.csv')
    df = pd.DataFrame({"time":[],"weight":[],"std":[]})
    df.columns = ["time","weight","std"]

    i = n
    count = 0
    while i < raw_data.size / 10:
        t = datetime(raw_data.iat[i,0],raw_data.iat[i,1],raw_data.iat[i,2],raw_data.iat[i,3],raw_data.iat[i,4],raw_data.iat[i,5],raw_data.iat[i,6])
        if exclude and raw_data.iloc[i-n:i].std().iloc[9] > thred:
            count += 1
            pass
        else:
            df.loc[len(df)] = [pd.to_datetime(t),raw_data.iloc[i,9],int(raw_data.iloc[i-n:i].std().iloc[9] >= thred)]
        i += 1
    print("Remove " + str(count) + " records.")
    df.plot.scatter(x = 'time', y = 'weight', c = 'std', cmap = 'viridis', s = 10)
    plt.show()

# std_filter()
# MA_filter(800)
# MA_filter(1000)
# MA_filter(1200)
# MA_filter(1600)
# MA_filter(40)
# cal_std(40)
draw_std(40,1,False)