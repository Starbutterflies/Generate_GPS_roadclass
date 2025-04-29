# 此文件用于处理数据。划分为逐秒的数据
import pandas as pd
import numpy as np
import os
import re
from utils import judge_begin_end


class trouble_dealer(object):
    def __init__(self, path):
        self.path = path
        self.name_list = os.listdir(self.path)
        self.path_list = [os.path.join(self.path, name) for name in self.name_list]  # 读取path_list
        self.data_list = []
        self.bool = True
        self.check_latitude = lambda text: bool(re.search("latitude", text, re.IGNORECASE))
        self.check_longitude = lambda text: bool(re.search("longitude", text, re.IGNORECASE))
        self.check_SPEED = lambda text: bool(re.search("speed", text, re.IGNORECASE))
        self.check_Date = lambda text: bool(re.search("date", text, re.IGNORECASE))
        self.check_Time = lambda text: bool(re.search("time", text, re.IGNORECASE))
        self.check_chinese_Time = lambda text: bool(re.search("时间", text))
        self.new_data_list = []
        self.time_stamp_data_list = []
        self.split_interpolate_data_list = []

    def read_data(self):
        """
        :return: 不返回任何东西。这段代码的作用是读取文件中的所有数据（未经处理）
        """
        csv_index_list = []
        excel_index_list = []
        for name in self.name_list:
            if (name.endswith('.csv')) or (name.endswith(".CSV")):
                csv_index_list.append(self.name_list.index(name))
            elif (name.endswith('.xls')) or (name.endswith(".xlsx")):
                excel_index_list.append(self.name_list.index(name))
        for index in csv_index_list:
            try:
                self.data_list.append(pd.read_csv(self.path_list[index]))
            except Exception as e:  # 看起来没有任何错误
                print(e)
        for index in excel_index_list:
            try:
                self.data_list.append(pd.read_excel(self.path_list[index]))
            except Exception as e:  # 看起来没有任何错误
                print(e)
        if len(self.data_list) == len(self.name_list):
            self.bool = True

    def rename_data(self):
        """
        给数据重命名,并整理为同一结构
        """
        if self.bool:
            for data in self.data_list:
                columns = data.columns
                if "TIME" in columns:
                    Date_index = [self.check_Date(column) for column in columns].index(True)
                    Time_index = [self.check_Time(column) for column in columns].index(True)
                    lat_index = [self.check_latitude(column) for column in columns].index(True)
                    long_index = [self.check_longitude(column) for column in columns].index(True)
                    speed_index = [self.check_SPEED(column) for column in columns].index(True)
                    new_column_index = [Date_index, Time_index, long_index, lat_index, speed_index]
                    new_column = [columns[i] for i in new_column_index]
                    new_data = data[new_column]
                    new_data.columns = ["Date", "Time", 'Longitude', 'Latitude', 'SPEED']
                    self.new_data_list.append(new_data)
                else:
                    date = data["时间"].apply(lambda x: x.split(" ")[0])
                    Time = data["时间"].apply(lambda x: x.split(" ")[1])
                    SPEED = data["SPEED"]
                    lat_index = [self.check_latitude(column) for column in columns].index(True)
                    long_index = [self.check_longitude(column) for column in columns].index(True)
                    new_column_index = [lat_index, long_index]
                    new_column = [columns[i] for i in new_column_index]
                    new_data = data[new_column]
                    new_data = new_data.assign(Date=date, Time=Time, SPEED=SPEED)
                    new_data.columns = ['Latitude', 'Longitude', "Date", "Time", 'SPEED']
                    new_data = new_data[["Date", "Time", 'Longitude', 'Latitude', 'SPEED']]
                    self.new_data_list.append(new_data)
        else:
            print(rf"请检查代码！数目不对！！！应该有{len(self.name_list)}个，而你读取了{len(self.data_list)}个")

    def judge_deal_data(self):
        """
        用于看数据是不是以0开头。是不是以0结尾
        将Longitude和Latitude结尾的E和N去掉
        检查时间戳是否连续和完整
        """
        i = 0
        for data in self.new_data_list:
            # 首先第一步，将SPEED、Lat和Long均转化为数值型
            try:
                data.loc[:, "Longitude"] = data["Longitude"].apply(lambda x: x.rstrip("E"))
                data.loc[:, "Latitude"] = data["Latitude"].apply(lambda x: x.rstrip("N"))
            except Exception as e:
                print(e)
            data = data.astype({"Latitude": np.float64, "Longitude": np.float64, "SPEED": np.float64})
            data = judge_begin_end(data)
            try:
                data["s"] = pd.to_datetime(data['Time'], format='%H%M%S').dt.time
                seconds_since_midnight = data['s'].apply(lambda x: x.hour * 3600 + x.minute * 60 + x.second)
                data['delta_time'] = seconds_since_midnight.diff()
                data['delta_time'].fillna(0, inplace=True)
            except Exception as e:
                data["s"] = pd.to_datetime(data['Time'].str.strip(), format='%H:%M:%S.%f').dt.strftime(
                    '%H:%M:%S')  # 也就是说。现在所有的时间就已经处理好啦！
                seconds_since_midnight = data['s'].apply(
                    lambda x: int(x.split(":")[0]) * 3600 + int(x.split(":")[1]) * 60 + int(x.split(":")[2]))
                data['delta_time'] = seconds_since_midnight.diff()
                data['delta_time'].fillna(1, inplace=True)  # 接下来一步是什么呢？
            self.time_stamp_data_list.append(data)

    def split_interpolate_data(self):
        """
        按照时间戳切割数据。将大段的时间切成连续的小段。但是出错了，我好像没更新i
        """
        for data in self.time_stamp_data_list:
            print(data)
            print("*" * 100)
            wrong_gps = data['delta_time'][abs(data['delta_time']) > 3]
            if len(wrong_gps) != 0:
                i = 0
                while i < len(wrong_gps)+1:
                    if i == 0:
                        new_data = data.loc[0:wrong_gps.index[i] - 1]
                    elif i == len(wrong_gps):
                        new_data = data.loc[wrong_gps.index[i - 1] + 1:]
                    else:
                        new_data = data.loc[wrong_gps.index[i-1] + 1:wrong_gps.index[i] - 1]

                    if len(new_data) > 1:
                        new_data = judge_begin_end(new_data)
                        if len(new_data) !=0:
                            if "-" in str(new_data["Date"].iloc[0]):
                                new_data.loc[:, "Datetime"] = pd.to_datetime(
                                    new_data['Date'].astype(str) + ' ' + new_data['Time'].astype(str))
                            else:
                                new_data["Datetime"] = pd.to_datetime(
                                    new_data['Date'].astype(str) + ' ' + new_data['Time'].astype(str),
                                    format='%y%m%d %H%M%S')
                            new_data = new_data.set_index('Datetime')
                            new_data.drop(['Date', 'Time', "s"], axis=1, inplace=True)
                            df_resampled = new_data.resample('S').mean().interpolate(method='linear')  # 现在就是已经经过插值的data了
                            self.split_interpolate_data_list.append(df_resampled)

                        else:
                            i += 1
                            continue
                        print(new_data)
                    i += 1

    def save_data(self, path):
        i = 0
        for data in self.split_interpolate_data_list:
            data.to_csv(os.path.join(path, rf"{i}.csv"))
            i += 1


if __name__ == '__main__':
    dealer = trouble_dealer(r"D:\Ancestors\data\天津数据\总数据")
    dealer.read_data()
    dealer.rename_data()

    dealer.judge_deal_data()
    dealer.split_interpolate_data()
    # dealer.save_data(r'./data')
