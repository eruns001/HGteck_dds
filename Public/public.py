import csv
from collections import defaultdict
import pandas as pd
import os.path
import datetime
import requests
import json
import sys
from multiprocessing import Pool, Manager
from itertools import repeat
import time
import threading
import random

api_key = 'mBC9%2FHjJoI52LSesUKliiF4nYyM7PKByjnnyEL3wcYIZlJdH2yWxogBR9%2FHYt2UxbkR2rRPyZ2F%2FAn70tbYlXA%3D%3D'

# RootPath = './data'
# Attach = 'wind'
# AttachPath = f'{RootPath}/{Attach}'
# CollectType = 'wind'
# NumberOfCollect = 24


class InitDict:
    # rootIndex + 1 = RootPath
    # index + 1 = Attach
    # index + 2 = collectType

    RootPath: str
    Attach: str
    AttachPath: str
    CollectType: str
    NumberOfCollect: int
    Now: datetime.datetime
    MB = 0.5196178778573866
    MK = 0.1741157739110656
    MS = 0.3062663482315478

    def __init__(self, collect_type: str, now: datetime.datetime = datetime.datetime.now()):
        try:
            configTxt_path = '../path.txt'
            configTxt = open(configTxt_path, 'r')
        except FileNotFoundError:
            configTxt_path = './path.txt'
            configTxt = open(configTxt_path, 'r')
        lines = configTxt.readlines()

        if not (collect_type == 'Wind' or collect_type == 'Dust' or collect_type == 'Traffic' or collect_type == 'Vssl'):
            collect_type = 'Wind'

        rootIndex = lines.index('RootPath\n')
        Index = lines.index(f'{collect_type}\n')

        root_path = lines[rootIndex + 1].strip().split(':')[-1]
        root_path.replace('\\', '/')
        attach = lines[Index + 1][:-1].strip().split(':')[-1]
        collect_type = lines[Index + 2][:-1].strip().split(':')[-1]

        # number of collect 상한 지정
        number_of_collect = int(lines[Index + 3][:-1].strip().split(':')[-1])
        if collect_type != 'Vssl' and number_of_collect >= 24:
            number_of_collect = 24
        if collect_type == 'Vssl' and number_of_collect >= 4:
            number_of_collect = 4
        attach_path = f'{root_path}/{attach}'

        if collect_type == 'Vssl':
            self.MB = float(lines[Index + 4][:-1].strip().split(':')[-1])
            self.MK = float(lines[Index + 5][:-1].strip().split(':')[-1])
            self.MS = float(lines[Index + 6][:-1].strip().split(':')[-1])

        self.RootPath = root_path
        self.Attach = attach
        self.AttachPath = attach_path
        self.CollectType = collect_type
        self.NumberOfCollect = number_of_collect
        self.Now = now


def url_to_response_text(url):
    headers = {'content-type': 'application/json;charset=utf-8'}
    response = requests.get(url, headers)
    responseText = response.text
    return responseText


def make_csv_path(encoding, item, init_dict: InitDict, is_pretreatment=False):
    attachPath = init_dict.AttachPath
    collectType = init_dict.CollectType
    month = init_dict.Now.strftime('%Y%m')
    if is_pretreatment:
        csvPath = f"{attachPath}/{collectType}_{encoding}_Pretreatment_{item}_{month}.csv"
    else:
        csvPath = f"{attachPath}/{collectType}_{encoding}_{item}_{month}.csv"
    return csvPath


def call_csv_last_index(csv_path: str, delta_time: str):
    delta: datetime.timedelta
    if delta_time == "days":
        delta = datetime.timedelta(days=1)
    elif delta_time == "hours":
        delta = datetime.timedelta(hours=1)
    else:
        delta = datetime.timedelta(hours=1)
    try:
        csvData: pd.DataFrame
        csvData = pd.read_csv(csv_path, index_col=0)
        csvLastIndex = csvData.index[-1]
        return csvLastIndex
    except Exception as e:
        now = datetime.datetime.now()
        an_hour_ago = now - delta
        return an_hour_ago.strftime("%Y%m%d%H")


def change_hour_1_to_24(date_time: datetime.datetime, check_type: str = "all") -> str:
    check_minute_bool: bool = True if (check_type == "all" or check_type == "minute") else False
    check_hour_bool: bool = True if (check_type == "all" or check_type == "hour") else False
    # check minute
    if int(date_time.strftime('%M')) < 40 and check_minute_bool:
        minute_checked = date_time + datetime.timedelta(hours=-1)
    else:
        minute_checked = date_time
    # check hour 1~24
    if minute_checked.strftime('%H') == '00' and check_hour_bool:
        _time = minute_checked - datetime.timedelta(hours=1)
        result = _time.strftime("%Y%m%d") + str(int(_time.strftime("%H")) + 1)
    else:
        result = minute_checked.strftime('%Y%m%d%H')
    return result


def change_YMDH_to_datetime(YMDH: str) -> datetime.datetime:
    print(f"YMDH:{YMDH}")
    YMDH = str(YMDH)
    print(type(YMDH))
    result = datetime.datetime(year=int(f"{YMDH[:4]}"), month=int(f"{YMDH[4:6]}"),
                               day=int(f"{YMDH[6:8]}"), hour=int(f"{YMDH[8:]}"))
    return result


def list_data_to_data_frame_data(type_list_data: list, columns: list):
    # It must to be
    # len(type_list_data) == len(columns)
    temp = dict()
    for index in range(len(type_list_data)):
        temp.setdefault(columns[index], [])
        temp[columns[index]].append(type_list_data[index])
    testDF = pd.DataFrame(temp)
    testDF.set_index('dataTime', inplace=True)
    print(f"pb testDF : \n{testDF}")
    return testDF


def update_csv(csv_path: str, data: pd.DataFrame):
    # 입력 DataFrame
    # path = csv path
    # DataFrame return
    result: pd.DataFrame
    # csv파일 있는지 확인. 있으면 업데이트
    if os.path.isfile(csv_path):
        csvData = pd.read_csv(csv_path, index_col=0)
        # check csv is null
        if csvData.index.size == 0:
            result = pd.concat([csvData, data], axis=0)
            result.to_csv(csv_path, index_label='dataTime')
            print(f"qwerqwer csvData.index.size == 0 \n{result}")
            return result
        else:
            csvLastIndex = csvData.index[-1]
            if int(csvLastIndex) != int(data.index[0]):
                result = pd.concat([csvData, data], axis=0)
                result.to_csv(csv_path, index_label='dataTime')
                print('updated')
                print(f"qwerqwer csvLastIndex != data.index[0] \n{result}")
                return result
            else:
                csvData.to_csv(csv_path, index_label='dataTime')
                result = csvData
                print(f"qwerqwer csvLastIndex == data.index[0] \n{result}")
                return result
    else:
        # folderPath 추출
        splitPathList = csv_path.split('/')
        folderPath = splitPathList[0]
        for eachPath in splitPathList[1:-1]:
            folderPath = f"{folderPath}/{eachPath}"

        # folder 없으면 생성
        if not os.path.isdir(folderPath):
            os.makedirs(folderPath)

        # csv 생성
        result = data
        result.to_csv(csv_path, index_label='dataTime')
        print(f"qwerqwer not os.path.isfile(csv_path) \n{result}")
        return result


def care_exception(e, folder_path, file_path):
    now = datetime.datetime.now()
    path_log = folder_path
    path_log_txt = file_path
    if not (os.path.isdir(path_log)):
        os.makedirs(os.path.join(path_log))
    if not (os.path.isfile(path_log_txt)):
        f = open(path_log_txt, 'w', encoding='utf-8')
        f.close()
    else:
        f = open(path_log_txt, 'a', encoding='utf-8')
        content = f"\nerror: {now.strftime('%Y%m%d  %H:%M')}\n\t- {e} -"
        f.write(content)
        f.close()
    print(f"error care_exception: {e}")
