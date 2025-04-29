import os

import pandas as pd
import numpy as np
import joblib  # 多进程执行
from joblib import Parallel, delayed
import re


def classify_data(total_df, real_df):
    """
    :param total_road_info: 总路的信息，在road_info里面
    :param real_road_info: 真实道路的信息，是data.test3中的内容
    :return: 处理后的数据.
    ps:需要进行一个微调.但是实际上很难绷。你怎么知道多的就是真的呢？？？！！！！！一座城市中有大量的同名道路。所以只能分割原来的数据了。妈的，还是需要人看！
    """
    road_one_list = []
    road_two_list = []
    for index, row in real_df.iterrows():
        road_one, road_two = (row["road_one"], row["road_two"])
        road_one_df = total_df[total_df["name"] == road_one]
        road_two_df = total_df[total_df["name"] == road_two]
        length1 = len(road_one_df)
        length2 = len(road_two_df)
        if (length1 != 0) and (length2 != 0):
            road_one_list.append(road_one_df["type"].value_counts().idxmax())
            road_two_list.append(road_two_df["type"].value_counts().idxmax())
        elif (length1 != 0) and (length2 == 0):
            road_one_list.append(road_one_df["type"].value_counts().idxmax())
            road_two_list.append(np.nan)
        elif (length1 == 0) and (length2 != 0):
            road_one_list.append(np.nan)
            road_two_list.append(road_two_df["type"].value_counts().idxmax())
        else:
            road_one_list.append(np.nan)
            road_two_list.append(np.nan)
    real_df["road_two_type"] = pd.DataFrame(road_two_list,index=real_df.index)
    real_df["road_one_type"] = pd.DataFrame(road_one_list,index=real_df.index)
    return real_df


def split_and_classify_data(real_road_path):
    """
    :param real_road_path: 将数据切分割为cpu数份
    :return: 切分后的数据
    """
    real_df = pd.read_csv(real_road_path)
    cpu_count = joblib.cpu_count()  # 然后将数据集分为这么多份。这样，如果说找不到名字的，先返回空值，然后按照上下的状况分辨下。如果说
    chunk = np.array_split(real_df, cpu_count)
    return chunk


def concat_new_position():
    """
    :return: 结合后的新csv，是两个地理数据集的（其实也没什么用处）
    """
    total_df1 = pd.read_excel("./road_info/export_layout.xls")[["name","fclass","ref"]]
    total_df2 = pd.read_csv("./road_info/Export_Output.csv")[["name","type","ref"]]
    total_df1.columns = ["name","type","ref"]  # 丢弃之后的信息
    total_df = pd.concat([total_df1,total_df2]).reset_index(drop=True)
    total_df.replace(' ',np.nan,inplace=True)
    total_df.dropna(subset="name",inplace=True)
    total_df.to_csv(r"./road_info/total_Export_Output.csv")


def return_to_origin(path):
    """
    :param path:保存的位置
    :return: 重新切割后的，连续时序的，并且分好类的数据集；实际上压根没有纯纯的高速路
    核心代码！！！
    """
    i = 0
    dataset = pd.read_csv(r"./data.test4.csv")
    lengths = [pd.read_csv(os.path.join(path,name)).shape[0] for name in os.listdir(path)]
    splits = []
    start = 0
    for length in lengths:
        if start < len(dataset):
            end = start + length
            splits.append(dataset[start:end])
            start = end
    for split in splits:
        if split[(split["road_one_type"].isin(["motorway","motorway_link","trunk","trunk_link"])) | (split["road_two_type"].isin(["motorway","motorway_link","trunk","trunk_link"]))].shape[0]/split.shape[0]<0.01:
            split.to_csv(rf"./normal_/{i}_normal_way.csv")
        elif split[(split["road_one_type"].isin(["motorway","motorway_link","trunk","trunk_link"])) | (split["road_two_type"].isin(["motorway","motorway_link","trunk","trunk_link"]))].shape[0]/split.shape[0]>0.9:
            split.to_csv(rf"./motor_/{i}_rapid_way.csv")
        else:
            split.to_csv(rf"./unclassified_/{i}.csv")
        i += 1


def time_split(path,**kwargs):
    begin_hour_m = kwargs["begin_time_m"][0]
    begin_minute_m = kwargs["begin_time_m"][1]
    end_hour_m = kwargs["end_time_m"][0]
    end_minute_m = kwargs["end_time_m"][1]

    begin_hour_e = kwargs["begin_time_e"][0]
    begin_minute_e = kwargs["begin_time_e"][1]
    end_hour_e = kwargs["end_time_e"][0]
    end_minute_e = kwargs["end_time_e"][1]
    df_list = [pd.read_csv(os.path.join(path,name)) for name in os.listdir(path)]
    for df in df_list:
        df.drop(['Unnamed: 0.2', 'Unnamed: 0.1'],inplace=True,axis=1)
        df['datetime_'] = pd.to_datetime(df['Datetime'])
        selected_data_m = df[(df['datetime_'].dt.hour >= begin_hour_m)  &
                           (df['datetime_'].dt.hour <= end_hour_m)]
        selected_data_e = df[(df['datetime_'].dt.hour >= begin_hour_e) & (df['datetime_'].dt.minute >= begin_minute_e) &
                           (df['datetime_'].dt.hour <= end_hour_e) & (df['datetime_'].dt.minute <= end_minute_e)]
        return selected_data_m,selected_data_e


def classify_unclassified_data(path):
    """
    :param path:
    第一步：填补空值：对于上下一致的，或者至少是同一种类型的，并且时间小于100s的，将中间认为也是同一种类型。如果无法判断，就用road2的信息。如果依然无法判断的尝试在表中进行模糊搜索。如果还是无法判断的，计算没有的长度，如果没那么长，说明影响不大，将其随意归为一类。
    如果确实有大段没法分类的情况，就将其留在未分类中，人工筛选
    第二步，切割。对于大段的，超过200s的片段，进行切割，并找到其前面所对应的0点和后面所对应的0点。
    第三步，将切割的小块放入motor中，将被切下来的放入
    :return:
    """
    unclassified_df_list = [pd.read_csv(os.path.join(path,name)).drop(['Unnamed: 0.2', 'Unnamed: 0.1', 'Unnamed: 0'], axis=1) for name in os.listdir(path)]
    for df in unclassified_df_list:
        print(df)
        break


if __name__ == '__main__':
    # chunks = split_and_classify_data(r'./data.test3.csv')
    # classify_df = pd.read_csv(r"./road_info/Export_Output.csv")
    # results = Parallel(n_jobs=16)(delayed(classify_data)(classify_df,chunk) for chunk in chunks) # 额实际上这个方法是有错的
    # new_position_df = pd.concat(results)
    # new_position_df.to_csv(r"./data.test4.csv")
    # concat_new_position()
    # return_to_origin("./data")
    # time_split("./normal_",begin_time_m=[7,0], end_time_m=[10,0], begin_time_e=[17,0], end_time_e=[20,0])
    classify_unclassified_data("./unclassified_")
    pass
