import os
import requests
import pandas as pd
import numpy as np
from utils import wgs84_to_gcj02
from tqdm import tqdm
from pyproj import Transformer
import time
import json
from joblib import Parallel, delayed
import random

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
           "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
           }


class generate_position(object):
    def __init__(self, path, total_point):
        self.path = path
        self.path_list = [os.path.join(path, name) for name in os.listdir(path)]
        self.df_list = [pd.read_csv(path) for path in self.path_list]  # 读取所有的df
        self.df = pd.concat(self.df_list).reset_index(drop=True)  # 拼接df
        self.total_point = total_point//16
        self.total_road_info = []
        self.key_list = ["请输入自己的高德APIkey！"]

    def request_data(self,df,i):
        """
        :return:返回的结果
        """
        latitude = df["Latitude"]
        longitude = df["Longitude"]
        step = latitude.shape[0] // self.total_point
        new_x = longitude[::step]
        new_y = latitude[::step]
        new_df = pd.DataFrame({'x': new_x, 'y': new_y})
        x_y_list = np.array(new_df).tolist()
        key = self.key_list[0]
        for x, y in x_y_list:
            try:
                location = str(x)+","+str(y)
                gcj02_response = requests.get(rf"https://restapi.amap.com/v3/assistant/coordinate/convert?key={key}&locations={location}&coordsys=gps")
                new_x,new_y = json.loads(gcj02_response.text)['locations'].split(",")
                time.sleep(0.2)
                response = requests.get(f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={new_x},{new_y}&key={key}&radius=300&extensions=all&roadlevel=1",headers=headers)
                self.total_road_info.append(response.text)
                print(response.text)
                time.sleep(0.2)
                pd.DataFrame([self.total_road_info]).to_csv(rf'./position_info/position_info{i}.csv', index=False)
            except Exception as e:
                if key == self.key_list[0]:
                    key = self.key_list[1]
                elif key == self.key_list[1]:
                    key = self.key_list[2]
                elif key == self.key_list[2]:
                    key = self.key_list[0]
                location = str(x) + "," + str(y)
                gcj02_response = requests.get(
                    rf"https://restapi.amap.com/v3/assistant/coordinate/convert?key={key}&locations={location}&coordsys=gps")
                new_x, new_y = json.loads(gcj02_response.text)['locations'].split(",")
                time.sleep(0.2)
                response = requests.get(
                    f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={new_x},{new_y}&key={key}&radius=300&extensions=all&roadlevel=1")
                self.total_road_info.append(response.text)
                print(response.text)
                time.sleep(0.2)
                pd.DataFrame([self.total_road_info]).to_csv(rf'./position_info/position_info{i}.csv', index=False)
            time.sleep(random.randint(2, 8))

    def job_lib_run(self):
        chunks = np.array_split(self.df,16)
        Parallel(n_jobs=16)(delayed(self.request_data)(chunk, i) for (i,chunk) in enumerate(chunks))


if __name__ == '__main__':
    # time.sleep(7200)
    position = generate_position(r'./data', 9000)
    print(position.df)
    position.job_lib_run()
