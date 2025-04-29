# 此文件用于填补空缺值
import pandas as pd
import numpy as np
import os
import json
from utils import deal_with_json
import time
import requests


def read_position_data(path):
    """
    :param file_name: 位置信息的路径
    :return: 读取后的数据，未填补空值。
    """
    file_name_list = [os.path.join(path,name) for name in os.listdir(path)]
    file_name_list = sorted(file_name_list, key=lambda x: int(x.split('\\')[-1].split('position_info')[-1].split('.csv')[0]))
    data = pd.concat([pd.DataFrame(pd.read_csv(file_name).T) for file_name in file_name_list]).reset_index(drop=True)
    data.columns = ["position"]
    data['position'] = data["position"].apply(lambda x: str(x) if x.find("regeocode") != -1 else r'{"status":"1","regeocode":{"roads":[],"roadinters":[]}}')
    data["road_one"] = data.apply(lambda x: json.loads(str(x["position"]))['regeocode']["roads"][0]["name"] if len(json.loads(str(x["position"]))['regeocode']["roads"])!=0 else np.nan, axis=1)
    data["road_two"] = data.apply(lambda x: json.loads(str(x["position"]))['regeocode']['roadinters'][0]['second_name'] if len(json.loads(str(x["position"]))['regeocode']['roadinters']) != 0 else np.nan, axis=1)
    return data


def read_origin_data(file_name):
    """
    :param file_name:
    :return: 读取的原始数据
    """
    names = os.listdir(file_name)
    total_df = pd.concat([pd.read_csv(os.path.join(file_name,name)) for name in names])  # 读取全部数据
    return total_df


def align_point(total_df,total_point,position_data):
    """
    :param total_df: 总的df
    :param total_point:  总的点
    :param position_data: 位置信息数据
    :return:
    """
    position_dfs = [df.reset_index(drop=True) for df in np.array_split(position_data,16)]
    chunks = np.array_split(total_df, 16)
    step_list = []
    real_point_list = []
    merged_df_list = []
    total_point = total_point // 16
    for chunk in chunks:
        step = chunk.shape[0] // total_point
        step_list.append(step)
        real_point_list.append(chunk.shape[0]//step)
    i = 0
    for step,chunk in zip(step_list,chunks):  # 实际上chunk分的非常的规整
        new_index = chunk[::step].index
        chunk[["position1", "position2"]] = np.ones([chunk.shape[0], 2])
        chunk[["position1", "position2"]] = chunk[["position1", "position2"]].astype(str)  # 将类型重新弄成str
        position_df = position_dfs[i]
        position_df.set_index(new_index,inplace=True)
        i += 1
        merged_df = pd.merge(chunk,position_df[["road_one","road_two"]],left_index=True,right_index=True,how='left')
        merged_df['road_one'] = merged_df['road_one'].fillna(merged_df['road_two'])
        merged_df['road_two'] = merged_df['road_two'].fillna(merged_df['road_one'])
        merged_df_list.append(merged_df)
    return pd.concat(merged_df_list).reset_index(drop=True)


def fill_na(merged_df):
    """
    :param merged_df:
    :return: 第一次填补的数据
    ps:实际上并不需要填补。因为实际上没有带来多余的信息。原来的就行。
    """
    lock = True
    i = 1
    for i in merged_df.index:
        if pd.isna(merged_df.loc[i, 'road_one']):
            prev_val = merged_df.loc[:i, 'road_one'].dropna().tail(1).values
            next_val = merged_df.loc[i:, 'road_one'].dropna().head(1).values
            if len(prev_val) > 0 and len(next_val) > 0 and prev_val[0] == next_val[0]:
                merged_df.loc[i, 'road_one'] = prev_val[0]
        if pd.isna(merged_df.loc[i, 'road_two']):
            prev_val = merged_df.loc[:i, 'road_two'].dropna().tail(1).values
            next_val = merged_df.loc[i:, 'road_two'].dropna().head(1).values
            if len(prev_val) > 0 and len(next_val) > 0 and prev_val[0] == next_val[0]:
                merged_df.loc[i, 'road_two'] = prev_val[0]
        i +=1
        print(i)
    merged_df.to_csv('./data.test1.csv', index=False)


def fill(fill_na_df):
    """
    :param fill_na_df:
    :return: 填补后的数据
    """
    one_fill = fill_na_df["road_one"].fillna(fill_na_df["road_two"])
    two_fill = fill_na_df["road_two"].fillna(fill_na_df["road_one"])
    fill_na_df["road_one"] = one_fill
    fill_na_df["road_two"] = two_fill
    return fill_na_df


def generate_new_position(fill_na_df):
    """
    :param fill_na_df: 需要填补的数据
    :return: 从中间取一个点，这里是第10个，凑成df并用api查找
    """
    empty_df = fill_na_df[fill_na_df["road_one"].isna()]
    new_empty_df = empty_df.loc[empty_df[empty_df["Unnamed: 0"].diff() != 1].index+10]
    key_list = ["d1906d59f7ec1a9a6d46b280eb53b04a","e5784fb3a14bcd797a6fa76868391523","33d52eccf62abc705583c16a311fb4a5"]
    new_df = new_empty_df[[ "Longitude","Latitude",]]
    x_y_list = np.array(new_df).tolist()
    key = key_list[0]
    total_road_info = []
    for x, y in x_y_list:
        try:
            location = str(x) + "," + str(y)
            gcj02_response = requests.get(
                rf"https://restapi.amap.com/v3/assistant/coordinate/convert?key={key}&locations={location}&coordsys=gps")
            new_x, new_y = json.loads(gcj02_response.text)['locations'].split(",")
            time.sleep(0.2)
            print(f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={new_x},{new_y}&key={key}&radius=300&extensions=all&roadlevel=1")
            response = requests.get(
                f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={new_x},{new_y}&key={key}&radius=300&extensions=all&roadlevel=1")
            total_road_info.append(response.text)
            print(response.text)
            time.sleep(0.2)
            pd.DataFrame([total_road_info]).to_csv(r'./position_info/position_info_empty.csv')
        except Exception as e:
            if key == key_list[0]:
                key = key_list[1]
            elif key == key_list[1]:
                key = key_list[2]
            else:
                key = key_list[0]
            location = str(x) + "," + str(y)
            gcj02_response = requests.get(
                rf"https://restapi.amap.com/v3/assistant/coordinate/convert?key={key}&locations={location}&coordsys=gps")
            new_x, new_y = json.loads(gcj02_response.text)['locations'].split(",")
            time.sleep(0.2)
            print(f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={new_x},{new_y}&key={key}&radius=300&extensions=all&roadlevel=1")
            response = requests.get(
                f"https://restapi.amap.com/v3/geocode/regeo?output=json&location={new_x},{new_y}&key={key}&radius=300&extensions=all&roadlevel=1")
            total_road_info.append(response.text)
            print(response.text)
            time.sleep(0.2)
            pd.DataFrame([total_road_info]).to_csv(r'./position_info/position_info_empty.csv')


def fill_empty(fill_na_df_,info_df):
    """
    :param fill_na_df_: 需填补的数据
    :param info_df: 新的位置数据
    :return: 终极的填补数据
    """
    empty_df = fill_na_df_[fill_na_df_["road_one"].isna()]
    empty_df.drop(['index'], axis=1, inplace=True)
    new_empty_df_index = empty_df.loc[empty_df[empty_df["Unnamed: 0"].diff() != 1].index + 10].index
    info_df = info_df.T.iloc[1:,]
    info_df.set_index(new_empty_df_index, inplace=True)
    info_df["road_one"] = info_df.apply(lambda x: json.loads(str(x[0]))['regeocode']["roads"][0]["name"] if len(json.loads(str(x[0]))['regeocode']["roads"])!=0 else np.nan, axis=1)
    info_df["road_two"] = info_df.apply(lambda x: json.loads(str(x[0]))['regeocode']['roadinters'][0]['second_name'] if len(json.loads(str(x[0]))['regeocode']['roadinters']) != 0 else np.nan, axis=1)
    one_ = info_df["road_one"].fillna(info_df["road_two"])
    two_ = info_df["road_two"].fillna(info_df["road_one"])
    info_df["road_one"] = one_
    info_df["road_two"] = two_
    info_df.drop([0], axis=1, inplace=True)   # 然后即为重新定位并进行填补
    for index, row in info_df.iterrows():
        index1 = index
        index2 = index
        upper_bool = True
        under_bool = True
        while (under_bool) or (upper_bool):
            index1 -= 1
            index2 += 1
            if not np.any(fill_na_df_.loc[index1].isna()):
                upper_bool = False
                index1 += 1
            try:
                if not np.any(fill_na_df_.loc[index2].isna()):
                    under_bool = False
                    index2 -= 1
            except:
                under_bool = False
                index2 -= 1
        fill_na_df_.loc[index1:index2].fillna(row["road_one"], inplace=True)
    return fill_na_df_


if __name__ == '__main__':
    # position_data = read_position_data(r"./position_info")
    # origin_data = read_origin_data(r"C:\Users\61912\PycharmProjects\分类并构建循环\data").reset_index()
    # merged_df = align_point(origin_data,9000,position_data)
    # fill_na(merged_df)
    fill_na_df = pd.read_csv(r"./data.test1.csv")
    fill_na_df = fill(fill_na_df)
    # fill_na_df.to_csv(r"./data.test2.csv")
    fill_na_df_ = pd.read_csv(r"./data.test2.csv")
    # generate_new_position(fill_na_df_)
    info_df = pd.read_csv(r"./position_info/position_info_empty.csv")
    fill_empty(fill_na_df_, info_df).to_csv(r"./data.test3.csv", index=False)

