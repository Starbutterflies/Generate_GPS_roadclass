from __future__ import division
import numpy as np
import pandas as pd
import pyproj
from pyproj import Transformer
import math
from math import pi, sqrt, sin, cos
from decimal import Decimal
from decimal import getcontext
import json


def judge_begin_end(data):
    if (data["SPEED"].iloc[0] == 0) and (data["SPEED"].iloc[-1] == 0):  # 首尾均为0。开始为0，结束不为0。结束为0.开始为0。开始和结束都不为0
        pass
    elif (data["SPEED"].iloc[0] != 0) or (data["SPEED"].iloc[-1] != 0):
        try:
            new_begin_index = data[data["SPEED"] == 0].index[0]
            new_end_index = data[data["SPEED"] == 0].index[-1]
            data = data.loc[new_begin_index:new_end_index].reset_index(drop=True)
        except Exception as e:
            data = pd.DataFrame()
    return data


def wgs84_to_gcj02(latitude, longitude):
    from pyproj import Transformer
    # 创建一个坐标系转换器，从wGS-84坐标系到6cj02坐标系
    transformer = Transformer.from_crs('epsg:4326', 'epsg:4490')
    # 将经度为15.12345，纬度为39.67890wGS-84坐标系转换为Gcj02坐标系
    gcj02_1ng, gcj02_lat = transformer.transform(latitude, longitude)
    return gcj02_1ng, gcj02_lat


x_pi = Decimal(3.14159265358979324) * Decimal(3000.0) / Decimal(180.0)
pi = Decimal(3.1415926535897932384626)  # π
a = Decimal(6378245.0)  # 长半轴
ee = Decimal(0.00669342162296594323)  # 扁率


def wgs84togcj02(lng, lat):
    """
    WGS84转GCJ02(火星坐标系)
    :param lng:WGS84坐标系的经度
    :param lat:WGS84坐标系的纬度
    :return:列表
    """
    dlat = transformlat(lng - 105.0, lat - 35.0)
    dlng = transformlng(lng - 105.0, lat - 35.0)
    radlat = lat / 180.0 * pi
    magic = math.sin(radlat)
    magic = 1 - ee * magic * magic
    sqrtmagic = math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * pi)
    dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * pi)
    mglat = lat + dlat
    mglng = lng + dlng
    return [mglng, mglat]


def transformlat(lng, lat):
    ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lat * pi) + 40.0 *
            math.sin(lat / 3.0 * pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(lat / 12.0 * pi) + 320 *
            math.sin(lat * pi / 30.0)) * 2.0 / 3.0
    return ret


def transformlng(lng, lat):
    ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(math.fabs(lng))
    ret += (20.0 * math.sin(6.0 * lng * pi) + 20.0 * math.sin(2.0 * lng * pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(lng * pi) + 40.0 * math.sin(lng / 3.0 * pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(lng / 12.0 * pi) + 300.0 * math.sin(lng / 30.0 * pi)) * 2.0 / 3.0
    return ret


def deal_with_json(row):
    road_list = json.loads(str(row["position"]))['regeocode']["roads"]
    if len(road_list) != 0:
        return road_list["name"]
    else:
        return np.nan


if __name__ == '__main__':
    data = pd.read_csv(r"D:\Ancestors\data\天津数据\总数据\20230703天津迈腾280-856-1054.CSV")
    wgs84_longitude = 116.480881
    wgs84_latitude = 39.989410
    print(wgs84_to_gcj02(wgs84_latitude, wgs84_longitude))
    # WGS84_Lat = 39.90872256455646
    # WGS84_Long = 116.3989789
    # print(transform(WGS84_Lat,WGS84_Long))
