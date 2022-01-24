import csv
from collections import defaultdict
import pandas as pd
import os.path
import datetime
import requests
import json
import sys
import time
import threading
import Public.public as pb
import collect.collect_wind as collect_wind

api_key = 'mBC9%2FHjJoI52LSesUKliiF4nYyM7PKByjnnyEL3wcYIZlJdH2yWxogBR9%2FHYt2UxbkR2rRPyZ2F%2FAn70tbYlXA%3D%3D'

initDict: pb.InitDict


# return jsonStringList
def get_data_list(nxny, urlTime, count):
    global initDict
    dayTime = urlTime.strftime('%Y%m%d')
    hourTime = urlTime.strftime("%H%M")

    dayUrl = dayTime
    hourUrl = f"{hourTime[:2]}00"

    url = f"http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst?serviceKey={collect_wind.api_key}&pageNo=1&numOfRows=120" \
          f"&base_date={dayUrl}&base_time={hourUrl}&nx={nxny[0]}&ny={nxny[1]}&dataType=JSON"
    # print(f"url:\n{url}")
    try:
        # get_url_start = datetime.datetime.now()
        jsonText = pb.url_to_response_text(url)
        get_item_wind = collect_wind.get_item(jsonText)
        if not get_item_wind:
            print(url)
            print(f"recall count : {count+1}")
            time.sleep(1)
            if count >= 2:
                time.sleep(5)
            return get_data_list(nxny, urlTime, count+1)

        # dataList[0] = TIME
        # change hour 01~24
        dataList = collect_wind.get_data_list_from_json(get_item_wind)
        if urlTime.strftime('%H') == '00':
            _time = urlTime - datetime.timedelta(hours=1)
            _timeString = _time.strftime("%Y%m%d") + str(int(_time.strftime("%H")) + 1)
            dataList[0] = _timeString

        # get_url_end = datetime.datetime.now()
        # print(f"url delay: {get_url_end - get_url_start}")
        return dataList
    except Exception as e:
        print(f"exception:\n{e}")
        print(url)
        sys.exit()


# create wind_csv, return wind_csv path
def create_wind_csv(nxny:str, encoding='euc-kr'):
    if encoding == 'utf-8':
        encoding = 'utf-8'
    elif encoding == 'euc-kr':
        encoding = 'euc-kr'
    else:
        encoding = 'utf-8'
    now = datetime.datetime.now().strftime('%Y%m')
    path = f"./data/{now}/wind"
    now_csv = datetime.datetime.now()
    month_csv = now_csv.strftime('%Y%m')
    path_csv = f'{path}/wind_{nxny}_{month_csv}.csv'

    # data 폴더 없으면 만들어짐
    if not (os.path.isdir(path)):
        os.makedirs(os.path.join(path))
        print("path created")

    else:
        print("yes path")

    if not (os.path.isfile(path_csv)):
        # 만들어진 폴더에 csv파일 생성
        f = open(path_csv, 'w', encoding=encoding)
        wr = csv.writer(f, lineterminator='\n')
        # csv파일 첫줄 입력
        wr.writerow(collect_wind.columns)
        f.close()
        print("csv created")
    else:
        print("yes csv")
    return path_csv


# 전처리 csv 파일 위치 return
def wind_csv_path(wind_data_type='vec'):
    rootPath = r"./data"
    now = datetime.datetime.now().strftime('%Y%m')
    path_folder = f"{rootPath}/{now}"
    path_wind = f"{path_folder}/{wind_data_type}_{now}.csv"

    # data 폴더 없으면 만들어짐
    if not (os.path.isdir(path_folder)):
        os.makedirs(os.path.join(path_folder))
        print("path created")

    if not (os.path.isfile(path_wind)):
        # 만들어진 폴더에 csv파일 생성
        f = open(path_wind, 'w', encoding='euc-kr')
        wr = csv.writer(f, lineterminator='\n')
        # csv파일 첫줄 입력
        nxnyList = []
        nxnyList.append(None)
        for x in range(93, 102 + 1):
            for y in range(71, 80 + 1):
                nxnyList.append(f"{x}_{y}")
        wr.writerow(nxnyList)
        f.close()
        print("file created")
    return path_wind


def update_csv(path, data, encoding='euc-kr'):
    f = open(path, 'a', encoding=encoding)
    wr = csv.writer(f, lineterminator='\n')
    # 데이터 삽입
    for oneHourData in data:
        wr.writerow(oneHourData)
    f.close()


# 수정 요
def merge_data(nxny, hourTime, return_dict):
    global initDict
    collectCount = initDict.NumberOfCollect-1
    dataForDay = []
    pinTime = hourTime - datetime.timedelta(hours=collectCount)
    moveTime = pinTime
    startTime = datetime.datetime.now()
    while moveTime <= hourTime:
        # 수정 요
        dataForHour = get_data_list(nxny, moveTime, 0)
        dataForDay.append(dataForHour)
        moveTime += datetime.timedelta(hours=1)
    endTime = datetime.datetime.now()
    print(nxny)
    print(f"delay 1day : {endTime - startTime}")
    return_dict[f"{nxny[0]}_{nxny[1]}"] = dataForDay
    print(f"merge_data dataForDay : {dataForDay}")


class MergeThread(threading.Thread):
    def __init__(self, nxny, hourTime, return_dict):
        threading.Thread.__init__(self)
        self.nxny = nxny
        self.hourTime = hourTime
        self.return_dict = return_dict

    def run(self):
        merge_data(self.nxny, self.hourTime, self.return_dict)


def wind_1st_test():
    global initDict
    initDict = pb.InitDict('Wind')
    # 시간 채크
    startTime = datetime.datetime.now()
    # 시간 통일
    mainTime = datetime.datetime.now()
    # check 50min
    if int(mainTime.strftime("%M")) < 40:
        mainTime = mainTime + datetime.timedelta(hours=-1)

    # 초단기 nx, ny
    nxnyList = []

    # # 원본 List
    # for x in range(93, 102 + 1):
    #     for y in range(71, 80 + 1):
    #         nxnyList.append(list([x, y]))

    # 테스트 위한 축약형
    for x in range(93, 97 + 1):
        for y in range(71, 74 + 1):
            nxnyList.append(list([x, y]))

    # # pool 제거 이전
    # # multiprocessing test
    # pool = Pool(processes=8)
    # returnDict = dict()
    # for nxny in nxnyList:
    #     returnDict[f"{nxny[0]}_{nxny[1]}"] = []
    # pool.starmap(merge_data, zip(nxnyList, repeat(mainTime), repeat(returnDict)))
    # # print(returnDict)
    # print(len(returnDict))

    # # pool 제거 임
    # returnDict = dict()
    # for nxny in nxnyList:
    #     returnDict[f"{nxny[0]}_{nxny[1]}"] = []
    # for nxny in nxnyList:
    #     merge_data(nxny, mainTime, returnDict)

    # Thread 사용버전
    threadStart = datetime.datetime.now()
    returnDict = dict()
    threadList = []
    for nxny in nxnyList:
        returnDict[f"{nxny[0]}_{nxny[1]}"] = []
    for nxny in nxnyList:
        time.sleep(1)
        mt = MergeThread(nxny, mainTime, returnDict)
        threadList.append(mt)
        mt.start()

    while sum([threadItem.is_alive() for threadItem in threadList]):
        time.sleep(1)
        print("not yet")

    threadEnd = datetime.datetime.now()
    print(f"소요시간 with thread : {threadEnd - threadStart}")

    # 전처리 이전 데이터
    pathList = []
    header = [None] + nxnyList
    for key in returnDict.keys():
        print(f"key:{key}")
        print(f"item:{returnDict[key]}")
        path = pb.make_csv_path('utf-8', key, initDict, False)
        tempDataFrameData = pb.list_data_to_data_frame_data(returnDict[key], columns=collect_wind.columns)
        print(f"tempDataFrameData : {tempDataFrameData}")
        # path = create_wind_csv(key, encoding='utf-8')
        update_csv(path, returnDict[key], encoding='utf-8')
        pathList.append(path)

    # 전처리
    # returnDict
    # key   val
    # nxny  ["dataTime", "UUU", "VVV", "VEC", "WSD", "PTY", "REH", "RN1", "T1H", "nx", "ny"] x 24

    # columnData
    # "UUU", "VVV", "VEC", "WSD", "PTY", "REH", "RN1", "T1H"
    columnData = [pd.DataFrame() for _ in collect_wind.columns[1:-2]]
    for key in returnDict.keys():
        singleNxNy = [pd.DataFrame() for _ in collect_wind.columns[1:-2]]
        # 시간대별
        for anhourData in returnDict[key]:
            for index in range(len(singleNxNy)):
                # data = UUU or VVV or VEC or ...
                # 실행 후 여기부터 다시 보기
                # collect_wind.columns[index+1] => key
                data = pd.DataFrame({key: anhourData[index+1]}, index=[anhourData[0]])
                # 시간대별로 세로로 데이터 넣는중
                singleNxNy[index] = pd.concat([singleNxNy[index], data], axis=0)

        # nxny 한줄씩 가로로 데이터 삽입
        for index in range(len(columnData)):
            columnData[index] = pd.concat([columnData[index], singleNxNy[index]], axis=1)

    for index in range(len(columnData)):
        column = collect_wind.columns[index+1]
        columnData[index].to_csv(wind_csv_path(column), encoding='euc-kr', index_label="dateTime")

    # 현 상태
    # columnData = DataFrame List
    # single DataFrame
    #       nxny
    # Date  value
    # Date  value

    endTime = datetime.datetime.now()
    print(f"delay total: {endTime - startTime}")


if __name__ == '__main__':
    wind_1st_test()
