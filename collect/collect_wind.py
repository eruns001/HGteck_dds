import csv
from collections import defaultdict
import pandas as pd
import os.path
import datetime
import requests
import json
import time
import Public.public as pb

api_key = 'mBC9%2FHjJoI52LSesUKliiF4nYyM7PKByjnnyEL3wcYIZlJdH2yWxogBR9%2FHYt2UxbkR2rRPyZ2F%2FAn70tbYlXA%3D%3D'

columns = ["dataTime", "UUU", "VVV", "VEC", "WSD", "PTY", "REH", "RN1", "T1H", "nx", "ny"]

initDict: pb.InitDict


# use with collect first
def get_item(json_data_str):
    try:
        json_data = json.loads(json_data_str)
        return json_data["response"]["body"]["items"]["item"]
    except json.JSONDecodeError:
        return False


# use with collect first
def get_data_list_from_json(json_list):
    itemList = ['' for _ in range(len(columns))]

    """
    UUU - 풍속(동서성분) m/s
    VVV - 풍속(남북성분) m/s
    VEC - 풍향 deg
    WSD - 풍속 m/s
    PTY - 강수형태
    REH - 습도 %
    RN1 - 1시간 강수량 mm
    T1H - 기온 ℃
    """

    start = time.perf_counter()
    for tempDict in json_list:
        # columns = ["dataTime", "UUU", "VVV", "VEC", "WSD", "PTY", "REH", "RN1", "T1H", "nx", "ny"]
        if tempDict["category"] in columns:
            # print(f"now test in :{tempDict['category']}")
            # print(f" index is : {columns.index(tempDict['category'])}")
            index = columns.index(tempDict['category'])
            itemList[index] = tempDict['obsrValue']
            itemList[-2] = tempDict['nx']
            itemList[-1] = tempDict['ny']
            itemList[0] = tempDict["baseDate"] + tempDict["baseTime"][:2]
    print(f"delay:{time.perf_counter() - start}")
    for index in range(len(itemList)):
        if float(itemList[index]) <= -998:
            itemList[index] = ''
    return itemList


def get_url(nxny) -> str:
    day_url = initDict.Now.strftime('%Y%m%d')
    # time check
    # if over 40min, hour -1
    time_url = initDict.Now.strftime("%H%M")
    hour_url = int(time_url[:2])
    base_time = str(hour_url - 1)
    if int(time_url[2:]) < 40:
        print(f"base time before :{base_time}")
        # 00시 오버플로우 제어
        if hour_url - 1 < 0:
            hour_url = hour_url + 24
            day_url = str(int(day_url)-1)
            base_time = str(hour_url - 1)
        # 한자리수 str화
        if hour_url - 1 < 10:
            base_time = f"0{base_time}"
            print(f"base time after :{base_time}")
        time_url = f"{base_time}00"

    url = f"http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst?serviceKey={api_key}&" \
          f"pageNo=1&numOfRows=120" \
          f"&base_date={day_url}&base_time={time_url}&nx={nxny[0]}&ny={nxny[1]}&dataType=JSON"
    print(f"geturl {nxny[0]}_{nxny[1]} : {url}")
    return url


# 1. nxny를 받음
# 2. 해당 nxny의 데이터를 csv로 저장.
# 3. 전처리를 위한 DataFrameList를 return
def call_ult_fcst_api(nxny):
    url = get_url(nxny)

    # path
    encoding = 'utf-8'
    wind_path = pb.make_csv_path(encoding, f'{nxny[0]}_{nxny[1]}', initDict, False)

    # call csv last index
    csv_last_index = pb.call_csv_last_index(wind_path, delta_time="hours")
    csv_last_datetime = pb.change_YMDH_to_datetime(csv_last_index)

    # change hour 01~24
    # def change_hour_1_to_24(??:datetime) -> str:
    # if initDict.Now.strftime('%H') == '00':
    #     _time = initDict.Now - datetime.timedelta(hours=1)
    #     _timeString = _time.strftime("%Y%m%d") + str(int(_time.strftime("%H")) + 1)
    # else:
    #     _timeString = initDict.Now.strftime('%Y%m%d%H')

    print("call_ult_fcst_api before")
    print(type(initDict.Now))

    _timeString = pb.change_hour_1_to_24(initDict.Now)

    print("call_ult_fcst_api after")

    try:
        responseText = pb.url_to_response_text(url)
        vilDataFrame = get_item(responseText)
        itemList = get_data_list_from_json(vilDataFrame)
        print(f"call_ult_fcst_api itemList : {itemList}")
        # change hour 01~24
        itemListDatetime = pb.change_YMDH_to_datetime(itemList[0])
        itemList[0] = pb.change_hour_1_to_24(itemListDatetime, "hour")

        # 다음 데이터인지 채크
        comparePinTime = pb.change_hour_1_to_24(csv_last_datetime + datetime.timedelta(hours=1))
        print(f"call_ult_fcst_api last datetime {comparePinTime}")
        print(f"call_ult_fcst_api last datetime {itemList[0]}")
        if comparePinTime != itemList[0]:
            itemList = [f"{_timeString}", '', '', '', '', '', '', '', '', nxny[0], nxny[1]]

    except Exception as e:
        now = datetime.datetime.now()
        this_month = now.strftime('%Y%m')
        this_day = now.strftime('%Y%m%d')
        path = initDict.AttachPath
        path_log = f"{path}/log/{this_month}"
        path_log_txt = f"{path_log}/{this_day}_{nxny[0]}_{nxny[1]}.txt"
        pb.care_exception(e, path_log, path_log_txt)
        print(f"Exception: \n {e}")

        # 빈데이터 처리 여기서 수정
        itemList = [f"{_timeString}", '', '', '', '', '', '', '', '', nxny[0], nxny[1]]

    print(f"call_ult_fcst_api itemList before list_data_to_data_frame_data : {itemList}")
    itemDF = pb.list_data_to_data_frame_data(type_list_data=itemList, columns=columns)
    pb.update_csv(wind_path, itemDF)

    returnList = [pd.DataFrame for _ in columns]
    for index in range(len(returnList)):
        # returnList 설명
        # 데이터 전처리를 위해서 각 column별로 데이터를 분리, DataFrame으로 변환, returnList에 저장
        # ex) UUU
        #           nx_ny
        # dateTime   data       x  len(column)
        returnList[index] = pd.DataFrame({f"{itemList[-2]}_{itemList[-1]}": itemList[index]},
                                         index=[itemList[0]])

    return returnList


def get_wind():
    global initDict
    initDict = pb.InitDict('Wind')
    # 초단기 nx, ny
    nxnyList = []
    # 시간
    initDict.Now = datetime.datetime.now()
    this_month = initDict.Now.strftime('%Y%m')

    single_day_list = [pd.DataFrame() for item in columns[1:-2]]

    for x in range(93, 102 + 1):
        for y in range(71, 80 + 1):
            nxnyList.append(list([x, y]))

    # # 테스트용
    # nxny = nxnyList[8]

    # # 에러확인용
    # singleData = call_ult_fcst_api(nxny=nxny)
    # # 1. nxny를 받음
    # # 2. 해당 nxny의 데이터를 csv로 저장.
    # # 3. 전처리를 위한 DataFrameList를 return
    # for index in range(len(single_day_list)):
    #     single_day_list[index] = pd.concat([single_day_list[index], singleData[index + 1]], axis=1)

    # # 구동 테스트
    # try:
    #     singleData = call_ult_fcst_api(nxny=nxny)
    #     # 1. nxny를 받음
    #     # 2. 해당 nxny의 데이터를 csv로 저장.
    #     # 3. 전처리를 위한 DataFrameList를 return
    #     for index in range(len(single_day_list)):
    #         single_day_list[index] = pd.concat([single_day_list[index], singleData[index + 1]], axis=1)
    # except Exception as e:
    #     this_day = initDict.Now.strftime('%Y%m%d')
    #     path_log = f"{initDict['RootPath']}/log/{this_month}"
    #     path_log_txt = f"{path_log}/{this_day}_{nxny[0]}_{nxny[1]}.txt"
    #     PB.care_exception(e, path_log, path_log_txt)

    # 실 구동용
    for nxny in nxnyList:
        # try:
        singleData = call_ult_fcst_api(nxny=nxny)
        # call_ult_fcst_api
        # 1. nxny를 받음
        # 2. 해당 nxny의 데이터를 csv로 저장.
        # 3. 전처리를 위한 DataFrameList를 return
        for index in range(len(single_day_list)):
            single_day_list[index] = pd.concat([single_day_list[index], singleData[index + 1]], axis=1)
        # except Exception as e:
        #     this_day = initDict.Now.strftime('%Y%m%d')
        #     path_log = f"{initDict.AttachPath}/log/{this_month}"
        #     path_log_txt = f"{path_log}/{this_day}_{nxny[0]}_{nxny[1]}.txt"
        #     pb.care_exception(e, path_log, path_log_txt)

    # get csv path
    pathList = [pb.make_csv_path(encoding='utf-8', item=item, init_dict=initDict, is_pretreatment=True)
                for item in columns[1:-2]]

    # single DataFrame fill null
    single_day_list = [item.fillna(value='') for item in single_day_list]

    # update Pretreatment csv
    for index in range(len(pathList)):
        pb.update_csv(pathList[index], single_day_list[index])


if __name__ == '__main__':
    get_wind()
