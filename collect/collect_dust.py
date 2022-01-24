import urllib.request
import urllib.parse
import requests
import json
import datetime
import os.path
import csv
import pandas as pd
import Public.public as pb

# 부산신항, 초량
api_key = 'mBC9%2FHjJoI52LSesUKliiF4nYyM7PKByjnnyEL3wcYIZlJdH2yWxogBR9%2FHYt2UxbkR2rRPyZ2F%2FAn70tbYlXA%3D%3D'
dustInfoDongList = ['태종대', '연산동', '장림동', '광복동', '덕천동', '부산신항', '청학동', '좌동', '당리동', '용수리',
                    '초량동', '대신동', '온천동', '덕포동', '전포동', '녹산동', '학장동', '대연동', '기장읍', '광안동',
                    '재송동', '화명동', '부곡동', '개금동', '명장동', '청룡동', '대저동', '부산북항', '수정동', '회동동',
                    '명지동', '백령도']
DongNumList = \
    ['2104066', '2113056', '2110060', '2101057', '2108058', '2112058', '2104063', '2109066', '2110055', '2131013',
     '2103053', '2102056', '2106056', '2115056', '2105083', '2112056', '2115063', '2107070', '2131011', '2114057',
     '2109064', '2108063', '2111058', '2105076', '2106063', '2111067', '2112051', '2103068', '2103056', '2111073',
     '2112059', '2332033']
# dustInfoList = ['dataTime', 'so2Value', 'coValue', 'o3Value', 'no2Value', 'pm10Value', 'pm10Value24', 'pm25Value',
#                 'pm25Value24', 'khaiValue', 'khaiGrade', 'so2Grade', 'coGrade', 'o3Grade', 'no2Grade', 'pm10Grade',
#                 'pm25Grade', 'pm10Grade1h', 'pm25Grade1h']

dustInfoList = ['dataTime', 'so2Value', 'coValue', 'o3Value', 'no2Value', 'pm10Value', 'pm25Value']

initDict: pb.InitDict


def get_url(station_name):
    # 시간단위 데이터 몇개 가져올지
    numOfRows = 1
    # 한글 인코딩
    encode = urllib.parse.quote_plus(station_name)

    url = f'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty?serviceKey={api_key}&' \
          f'numOfRows={numOfRows}&pageNo=1&stationName={encode}&dataTerm=DAILY&ver=1.3&returnType=json'
    print(url)
    return url


def get_item(json_data_str):
    json_data = json.loads(json_data_str)
    return json_data["response"]["body"]["items"]


# 한시간치 데이터 List로 return
def json_to_list(json_list):
    itemList = ['' for _ in range(len(dustInfoList))]
    print(f"dust_info json_list:\n{json_list}")
    for i in range(len(itemList)):
        # dustInfoList : json key list
        json_item = json_list[dustInfoList[i]]
        if dustInfoList[i] == 'dataTime':
            json_item = f"{json_item[:4]}{json_item[5:7]}{json_item[8:10]}{json_item[11:13]}"
            print(f"_temp:{json_item}")
        itemList[i] = json_item
    # "-" 수정
    for index in range(len(itemList)):
        if itemList[index] == '-' or itemList[index] == "-":
            itemList[index] = ''
    print(f"dust_info itemList:\n{itemList}")
    # ex) dust_info itemList : ['2021120613', '0.006', '0.3', '0.028', '0.024', '27', '10']
    return itemList


# 1. list_index, dong을 받음
# 2. 해당 list_index, dong의 데이터를 csv로 저장.
# 3. 전처리를 위한 DataFrameList를 return
def call_dust_url(list_index, dong):
    global initDict
    path = initDict.AttachPath
    dong_name = dustInfoDongList[list_index]
    dong_id = DongNumList[list_index]
    now = initDict.Now
    # item Data Frame List, 순서 = dustInfoList
    single_data_frame_list = []
    url = get_url(dong_name)

    # path
    encoding = 'utf-8'
    dust_hour_csv_path = pb.make_csv_path(encoding, f'{dong}', initDict, False)

    # call csv last index
    csv_last_index = pb.call_csv_last_index(dust_hour_csv_path, delta_time="hours")
    csv_last_datetime = pb.change_YMDH_to_datetime(csv_last_index)

    _timeString = pb.change_hour_1_to_24(initDict.Now)

    try:
        responseText = pb.url_to_response_text(url)
        dust_json = get_item(responseText)
        dustHourList = json_to_list(dust_json[0])
        # dustInfoList: ['dataTime', 'so2Value', 'coValue', 'o3Value', 'no2Value', 'pm10Value', 'pm25Value']
        # dustHourList: ['2021120613', '0.006', '0.4', '0.015', '0.030', '34', '22']
        HourListDateTime = pb.change_YMDH_to_datetime(dustHourList[0])
        dustHourList[0] = pb.change_hour_1_to_24(HourListDateTime)

        # 다음 데이터인지 채크
        comparePinTime = pb.change_hour_1_to_24(csv_last_datetime + datetime.timedelta(hours=1))
        print(f"call_ult_fcst_api last datetime {comparePinTime}")
        print(f"call_ult_fcst_api last datetime {dustHourList[0]}")
        if comparePinTime != dustHourList[0]:
            dustHourList = [f"{_timeString}", '', '', '', '', '', '']

    # api 에서 데이터를 받지 못한 경우
    except Exception as e:
        print(f"call dust url dust {now.strftime('%Y%m%d %H:%M')}\n{e}")
        # 시간
        this_month = now.strftime('%Y%m')
        this_day = now.strftime('%Y%m%d')
        path_log = f"{path}/log/{this_month}"
        path_log_txt = f"{path_log}/{this_day}_{dong}.txt"
        pb.care_exception(e, path_log, path_log_txt)

        # 빈데이터 처리 여기서 수정
        dustHourList = ['' if index <= 9 else 'error' for index in range(len(dustInfoList))]

        dustHourList[0] = pb.change_hour_1_to_24(now)

    # list_index, dong의 데이터를 csv로 저장.
    dustHourDF = pb.list_data_to_data_frame_data(dustHourList, dustInfoList)
    encoding = 'utf-8'
    dust_path = pb.make_csv_path(encoding, dong, initDict, False)
    pb.update_csv(dust_path, dustHourDF)

    # 항목별 csv에 저장
    # 빈데이터 후처리
    # dustHourList: ['2021120613', '0.006', '0.4', '0.015', '0.030', '34', '22']
    print(f"asdf single_day_data : \n{dustHourList}")

    for item in range(len(dustHourList[1:])):
        index = item+1
        # item Data Frame 생성
        #       dong_id
        # date  val
        single_data_frame = pd.DataFrame({dong_id: dustHourList[index]}, index=[dustHourList[0]])
        print(f"asdf single_data_frame {single_data_frame}")
        single_data_frame_list.append(single_data_frame)

    return single_data_frame_list


def get_dust():
    global initDict
    initDict = pb.InitDict('Dust')

    # 빈 리스트 생성
    dustInfoDataFrameList = [pd.DataFrame() for _ in range(len(dustInfoList)-1)]
    for index_list in range(len(dustInfoDongList)):
        # url 호출은 동 이름으로
        dong = dustInfoDongList[index_list]
        try:
            # call_dust_url:
            # 1. list_index, dong을 받음
            # 2. 해당 list_index, dong의 데이터를 csv로 저장.
            # 3. 전처리를 위한 DataFrameList를 return
            dustInfoDataFrameListDong = call_dust_url(list_index=index_list, dong=dong)
            print(f"adsf dustInfoDataFrameListDong {dustInfoDataFrameListDong}")
            # 각 항목별로 정리
            for index in range(len(dustInfoDataFrameListDong)):
                dustInfoDataFrameList[index] = pd.concat(
                    [dustInfoDataFrameList[index], dustInfoDataFrameListDong[index]], axis=1)
        # 예외 로그 txt로 저장
        except Exception as e:
            # 시간
            now = datetime.datetime.now()
            this_month = now.strftime('%Y%m')
            this_day = now.strftime('%Y%m%d')
            path = initDict.AttachPath
            path_log = f"{path}/log/{this_month}"
            path_log_txt = f"{path_log}/{this_day}_{dong}.txt"

            pb.care_exception(e, path_log, path_log_txt)

    # dustInfoDataFrameList = 항목별 정리된 List
    # ex) coFlag, pm10Flag, ...
    for index in range(len(dustInfoDataFrameList)):
        print(f"dustInfoDataFrameList - {dustInfoList[1:][index]} : \n{dustInfoDataFrameList[index]}")

    # get csv path
    pathList = [pb.make_csv_path(encoding='utf-8', item=item, init_dict=initDict, is_pretreatment=True) for item in dustInfoList[1:]]

    # single DataFrame fill null
    for index in range(len(dustInfoDataFrameList)):
        dustInfoDataFrameList[index] = dustInfoDataFrameList[index].fillna(value='')

    for index in range(len(pathList)):
        pb.update_csv(pathList[index], dustInfoDataFrameList[index])


if __name__ == '__main__':
    get_dust()
