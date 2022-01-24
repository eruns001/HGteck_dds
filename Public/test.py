import datetime
import os
import pandas as pd
import Public.public as pb

TestCsvData = [2021112412, 0.002, 0.3, 0.028, 0.011, 21, 13]
ColumnsDust = ['dataTime', 'so2Value', 'coValue', 'o3Value', 'no2Value', 'pm10Value', 'pm25Value']
ColumnsWind = ["dataTime", "UUU", "VVV", "VEC", "WSD", "PTY", "REH", "RN1", "T1H", "nx", "ny"]
listTemp = [['2022012313', '-1.9', '0.1', '92', '2', '0', '47', '0', '11.8', 93, 73],
            ['2022012314', '-0.9', '0.6', '122', '1.2', '0', '45', '0', '11.9', 93, 73],
            ['2022012315', '-0.9', '-0.2', '75', '1', '0', '45', '0', '11.7', 93, 73],
            ['2022012316', '-0.6', '0.8', '139', '1.1', '0', '48', '0', '11.1', 93, 73],
            ['2022012317', '-0.4', '-0.7', '33', '0.9', '0', '61', '0', '8.5', 93, 73],
            ['2022012318', '-0.5', '-0.3', '55', '0.7', '0', '65', '0', '7.3', 93, 73],
            ['2022012319', '-3.7', '-1.2', '71', '4', '0', '65', '0', '9.6', 93, 73],
            ['2022012320', '-2.6', '-0.8', '71', '2.9', '0', '67', '0', '8.9', 93, 73],
            ['2022012321', '-3.1', '0.2', '93', '3.2', '0', '66', '0', '8.7', 93, 73],
            ['2022012322', '-5', '0.1', '91', '5.1', '0', '67', '0', '8.6', 93, 73],
            ['2022012323', '-3.1', '0', '90', '3.2', '0', '70', '0', '8.1', 93, 73],
            ['2022012324', '-1.4', '0.3', '100', '1.5', '0', '68', '0', '7.8', 93, 73],
            ['2022012401', '-2.2', '-0.3', '81', '2.3', '0', '62', '0', '7.9', 93, 73],
            ['2022012402', '-2.9', '0.7', '103', '3.1', '0', '64', '0', '7.7', 93, 73],
            ['2022012403', '-2.8', '0.5', '100', '2.9', '0', '65', '0', '7.5', 93, 73],
            ['2022012404', '-3.8', '1.1', '106', '4.1', '0', '64', '0', '7.3', 93, 73],
            ['2022012405', '-2.3', '0.4', '99', '2.4', '0', '63', '0', '6.6', 93, 73],
            ['2022012406', '-2', '0', '89', '2.1', '0', '61', '0', '6.2', 93, 73],
            ['2022012407', '-2.4', '1.3', '119', '2.8', '0', '54', '0', '6.3', 93, 73],
            ['2022012408', '-4', '1.6', '112', '4.4', '0', '50', '0', '6.6', 93, 73]]


def list_data_to_data_frame_data(type_list_data: list, columns: list):
    temp = dict()
    for index in range(len(type_list_data)):
        temp.setdefault(columns[index], [])
        temp[columns[index]].append(type_list_data[index])
    testDF = pd.DataFrame(temp)
    testDF.set_index('dataTime', inplace=True)
    return testDF


def test():
    # temp = PB.init_read_path('Wind')
    # if not (os.path.isdir(temp)):
    #     os.makedirs(os.path.join(temp))
    #     print("path created")
    path = '//_data223f/windT/test.csv'
    testCsv = pd.read_csv(path, index_col=0)
    # Last Index of Csv : testCsv.index.values[-1]
    print(testCsv.index.values[-1])
    data = list_data_to_data_frame_data(TestCsvData, Columns)
    if testCsv.index.values[-1] != data.index.values[0]:
        result = pd.concat([testCsv, data], axis=0)
        result.to_csv(path)
        print('updated')
    # print(result)


if __name__ == '__main__':
    #listTemp
    pbTemp = pb.list_data_to_data_frame_data(listTemp, ColumnsWind)
    print(pbTemp)
    # print(df.replace(''))
