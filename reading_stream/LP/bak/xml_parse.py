import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import numpy as np
import pandas as pd
from lxml import etree

from ami import constant, func, lp_config
from ami.constant import LOADPROFILE

"""
4/14 mn_xml_file parsing using xml.etree.ElementTree
4/20 new using dictionary to store
"""


def parse_xml(xml_file: str, file_name: str, gz_path=None) -> tuple:
    """
    Parse the xml file and return the dataframe

    Args:
        xml_file (str): xml file path
        file_name (str): file name
        gz_name (str): gz file name

    Returns:
        tuple: dataframe, error dataframe, warning dataframe, file info
    """
    error_log = []
    file_group_name, file_uuid, *_ = file_name.split("_")

    # init.
    rtc_look_table = LOADPROFILE

    # stored 讀表資料
    header_map = {}
    read_map = {}

    # sys max value
    max_double = np.finfo(np.float64).max  # equal double in java
    max_double_str = np.format_float_positional(max_double, trim="-")
    max_float = np.finfo(np.float32).max
    max_float_str = np.format_float_positional(max_float, trim="-")
    max_bigint = str(sys.maxsize)
    max_int = str(np.iinfo(np.int32).max)

    # create element tree object
    try:
        tree = etree.parse(xml_file, parser=None)
        root = tree.getroot()

        # Get Namespace
        ns = {"ns": "http://iec.ch/TC57/2011/schema/message"}  # header
        ns2 = {"ns2": "http://iec.ch/TC57/2011/MeterReadings#"}  # payload


        # ---------------------------------------------------------------------------- #
        #                                Get Header Info                               #
        # ---------------------------------------------------------------------------- #
        source = root.find(".//ns:Source", ns).text
        msg_id = root.find(".//ns:MessageID", ns).text
        corr_id = root.find(".//ns:CorrelationID", ns).text
        msg_time = root.find(".//ns:Timestamp", ns).text
        read_group = root.find('ns:Property/ns:Name[text()="ReadingTypeGroup"]/../ns:Value', ns).text
        verb = root.find(".//ns:Verb", ns).text
        noun = root.find(".//ns:Noun", ns).text
        context = root.find(".//ns:Context", ns).text
        msg_idx = root.find('ns:Property/ns:Name[text()="MessageIndex"]/../ns:Value', ns).text
        rev = root.find(".//ns:Revision", ns).text
        qos = root.find('ns:Property/ns:Name[text()="QOSLevel"]/../ns:Value', ns).text

        header_tmp = {
                "source": source,
                "msg_id": msg_id,
                "corr_id": corr_id,
                "msg_time": msg_time,
                "read_group": read_group,
                "verb": verb,
                "noun": noun,
                "context": context,
                "msg_idx": msg_idx,
                "rev": rev,
                "qos": qos
        }
        # try:
        #     lp_config.header_map(header_tmp)
        # except Exception as e:
        #     print(e.errors)


        # ---------------------------------------------------------------------------- #
        #                               Get Payload Info                               #
        # ---------------------------------------------------------------------------- #
        meter_reading_list = root.find(".//ns2:MeterReadings", ns2)
        mRID = ""
        meter_unique_id = ""
        fan_unique_id = ""
        read_date_time = ""  # 讀表值產生時間 ISO8601格式
        interval_state = 1  # 間距狀態代碼(1:normal, 2:short, 3:long, 4:invalid); default=1
        interval_state_note = 1  # 間距狀態原因註記代碼(1:無, 2:time changed校時, 3:power down斷電, 4:fault電表故障, 5:work施工有電, 6:power-down施工無電, 7:手工輸入); default=1
        record_no = None

        # ------------------------------ 1.meter_reading ----------------------------- #
        for meter_reading in meter_reading_list.findall("ns2:MeterReading", ns2):
            meter = meter_reading.find("ns2:Meter", ns2)
            mRID = meter.find("ns2:mRID", ns2).text  # 電表uuid
            # total_cnt += 1

            # 處理meter_unique_id & fan_unique_id
            name = meter.find(".//ns2:Names/ns2:NameType/ns2:name", ns2).text
            value = meter.find(".//ns2:Names/ns2:name", ns2).text

            if name == "MeterUniqueID":
                meter_unique_id = value
            if name == "FanUniqueID":
                fan_unique_id = value

            interval_blocks_list = meter_reading.findall("ns2:IntervalBlocks", ns2)

            # ------------------------------ 2.intervalBlock ----------------------------- #
            for intervalBlock in interval_blocks_list:
                # read_err_msg_map_temp = {} #暫存當筆Reading的檢查錯誤 [type:message, ...]
                reading_type_code = ""
                if (
                    None == intervalBlock.find("ns2:ReadingType", ns2)
                    or intervalBlock.find("ns2:ReadingType", ns2).attrib["ref"].strip() == ""
                ):
                    # read_err_msg_map_temp["E3001"] = "E3001 表號: {}的ReadingTypeCode未填".format(meter_unique_id)
                    error_log.append(
                        {
                            "file_type": file_type,
                            "file_path": file_path,
                            "raw_gzfile": raw_gzfile,
                            "raw_file": file_name,
                            "rec_time": rec_time,
                            "source": source,
                            "read_group": read_group,
                            "meter_id": meter_unique_id,
                            "read_time": None,  # ReadingTypeCode未填，無read_time
                            "type_cd": "E23001",
                            "col_nm": None,
                            "log_upd_time": datetime.now(),
                        }
                    )
                    error_cnt += 1

                else:
                    _reading_type_code = intervalBlock.find("ns2:ReadingType", ns2).attrib["ref"]
                    if _reading_type_code in rtc_look_table:
                        reading_type_code = _reading_type_code
                        # # 亞太&遠傳還沒將TOUID RTC改為正確:"11.0.0.0.0.2.167.0.0.0.0.0.0.0.0.0.0.0"
                        # if _reading_type_code == '0.0.0.0.0.2.167.0.0.0.0.0.0.0.0.0.0.0':
                        #     reading_type_code = '11.0.0.0.0.2.167.0.0.0.0.0.0.0.0.0.0.0'
                    else:
                        # read_err_msg_map_temp["E3002"] = "E3002 ReadingTypeCode:''{}''不合規範".format(_reading_type_code) #不跳出, 繼續檢查其他錯誤
                        error_log.append(
                            {
                                "file_type": file_type,
                                "file_path": file_path,
                                "raw_gzfile": raw_gzfile,
                                "raw_file": file_name,
                                "rec_time": rec_time,
                                "source": source,
                                "read_group": reading_type_group,
                                "meter_id": meter_unique_id,
                                "read_time": None,  # ReadingTypeCode未填，無read_time
                                "type_cd": "E23002",
                                "col_nm": None,
                                "log_upd_time": datetime.now(),
                            }
                        )
                        error_cnt += 1

                interval_readings_list = intervalBlock.findall("ns2:IntervalReadings", ns2)
                # 3.interval_reading
                for interval_reading in interval_readings_list:
                    # 處理Readtime
                    read_date_time = None
                    if interval_reading.find("ns2:timeStamp", ns2) == None:
                        # read_err_msg_map_temp["E3003"] = "E3003 ReadTime未填" #interval時戳未填
                        error_log.append(
                            {
                                "file_type": file_type,
                                "file_path": file_path,
                                "raw_gzfile": raw_gzfile,
                                "raw_file": file_name,
                                "rec_time": rec_time,
                                "source": source,
                                "read_group": reading_type_group,
                                "meter_id": meter_unique_id,
                                "read_time": None,  # ReadingTypeCode錯誤
                                "type_cd": "E23003",
                                "col_nm": None,
                                "log_upd_time": datetime.now(),
                            }
                        )
                        error_cnt += 1

                    else:
                        pattern = r"^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$"
                        if (
                            re.match(pattern, interval_reading.find("ns2:timeStamp", ns2).text)
                            and len(interval_reading.find("ns2:timeStamp", ns2).text) >= 19
                        ):
                            read_date_time = interval_reading.find("ns2:timeStamp", ns2).text

                            # 跳過DAS 1/19 & 1/20
                            # if source == 'HES-DAS20180705':
                            #     if read_date_time[0:10] in ['2023-01-19','2023-01-20']:
                            #         continue
                        else:
                            # read_err_msg_map_temp["E3004"] = "E3004 ReadTime:'{}'不是合法的ISO8601格式".format(read_date_time)
                            error_log.append(
                                {
                                    "file_type": file_type,
                                    "file_path": file_path,
                                    "raw_gzfile": raw_gzfile,
                                    "raw_file": file_name,
                                    "rec_time": rec_time,
                                    "source": source,
                                    "read_group": reading_type_group,
                                    "meter_id": meter_unique_id,
                                    "read_time": read_date_time,
                                    "type_cd": "E23004",
                                    "col_nm": None,
                                    "log_upd_time": datetime.now(),
                                }
                            )
                            error_cnt += 1

                    # 4.處理ReadingQualities
                    read_quality_list = interval_reading.findall("ns2:ReadingQualities", ns2)
                    for read_quality in read_quality_list:
                        match read_quality.find("ns2:ReadingQualityType", ns2).attrib["ref"]:
                            case "1.0.0":  # 讀值正確
                                interval_state = 1
                                interval_state_note = 1
                            case "1.4.2001":  # Partial/Short Interval+校時原因
                                interval_state = 2
                                interval_state_note = 2
                            case "1.4.2002":  # Partial/Short Interval+斷復電原因
                                interval_state = 2
                                interval_state_note = 3
                            case "1.4.3001":  # Long Interval+校時原因
                                interval_state = 3
                                interval_state_note = 2
                            case "1.4.3002":  # Long Interval+斷復電原因
                                interval_state = 3
                                interval_state_note = 3
                            case "1.5.257":  # 無效資料
                                interval_state = 4
                                interval_state_note = 1
                            case "2.7.1":  # 人工輸入
                                interval_state = 1
                                interval_state_note = 7
                            case "5.4.260":  # Record Number
                                if read_quality.find("ns2:comment", ns2) == None:
                                    # read_err_msg_map_temp["E3005"] = "E3005 RecordNumber未填"
                                    error_log.append(
                                        {
                                            "file_type": file_type,
                                            "file_path": file_path,
                                            "raw_gzfile": raw_gzfile,
                                            "raw_file": file_name,
                                            "rec_time": rec_time,
                                            "source": source,
                                            "read_group": reading_type_group,
                                            "meter_id": meter_unique_id,
                                            "read_time": read_date_time,
                                            "type_cd": "E23005",
                                            "col_nm": None,
                                            "log_upd_time": datetime.now(),
                                        }
                                    )
                                    error_cnt += 1

                                else:
                                    pattern = r"^\d{1,5}$"
                                    record_no_str = read_quality.find("ns2:comment", ns2).text
                                    if re.match(pattern, record_no_str):
                                        record_no = int(record_no_str)
                                    else:
                                        # read_err_msg_map_temp["E3006"] = "E3006 RecordNumber:''{}''不合規範".format(record_no_str)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23006",
                                                "col_nm": None,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1

                    # 轉換讀表值為適當的資料格式
                    value = None
                    reading_name = ""
                    reading_data_type = None

                    if None != reading_type_code:
                        lookup_map = rtc_look_table[reading_type_code]
                        reading_name = lookup_map["name"]
                        reading_data_type = lookup_map["type"]

                        if (
                            None == interval_reading.find("ns2:value", ns2)
                            or interval_reading.find("ns2:value", ns2).text == None
                        ):
                            if reading_data_type == "timestamp":
                                value = "1900-01-01T00:00:00.000+08:00"  # HES回傳為"None"轉成null, (eip轉為"1900-01-01T00:00:00.000+08:00")

                            error_log.append(
                                {
                                    "file_type": file_type,
                                    "file_path": file_path,
                                    "raw_gzfile": raw_gzfile,
                                    "raw_file": file_name,
                                    "rec_time": rec_time,
                                    "source": source,
                                    "read_group": reading_type_group,
                                    "meter_id": meter_unique_id,
                                    "read_time": read_date_time,
                                    "type_cd": "E23011",  # value 未填
                                    "col_nm": reading_name,
                                    "log_upd_time": datetime.now(),
                                }
                            )
                            error_cnt += 1
                            continue
                        else:
                            value_str = interval_reading.find(
                                "ns2:value", ns2
                            ).text.strip()  # 原始讀表值

                        # if(value_str == None):
                        #     print('{}{}: {} is null'.format(reading_name,reading_type_code,value_str))

                        # 檢查reading_data_type
                        try:
                            match reading_data_type:
                                case "bigint":
                                    value_str_split = value_str.split(".")
                                    # 檢查中華一期整數送小數問題(僅顯示不跳出)
                                    # if len(value_str_split)>1:
                                    #     print("{}({}): '{}'不是合法的{}格式({})".format(reading_name,reading_type_code,value_str,reading_data_type,source))

                                    # 檢查是否為數字型態
                                    pattern = r"^(\+|-)?\d+$"
                                    if not re.match(pattern, value_str_split[0]):
                                        # read_err_msg_map_temp["E3007"] = "E3007 {}({}): '{}'不是合法的{}格式".format(reading_name,reading_type_code,value_str,reading_data_type)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23007",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue  # ignore
                                    # 檢查是否逾可接受的最大值
                                    temp = re.sub("^0*", "", value_str_split[0])  # 去除開頭的'0'
                                    if len(max_bigint) < len(temp):
                                        # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_bigint)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23008",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue
                                    elif len(max_bigint.split(".")[0]) == len(temp):
                                        if max_bigint < temp:
                                            # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_bigint)
                                            error_log.append(
                                                {
                                                    "file_type": file_type,
                                                    "file_path": file_path,
                                                    "raw_gzfile": raw_gzfile,
                                                    "raw_file": file_name,
                                                    "rec_time": rec_time,
                                                    "source": source,
                                                    "read_group": reading_type_group,
                                                    "meter_id": meter_unique_id,
                                                    "read_time": read_date_time,
                                                    "type_cd": "E23008",
                                                    "col_nm": reading_name,
                                                    "log_upd_time": datetime.now(),
                                                }
                                            )
                                            error_cnt += 1
                                            continue
                                    # 檢查是否為負值
                                    value = value_str_split[0]  # int類型給予字串避免dataframe轉型為float
                                    # if 0 > value:
                                    #     print("{}({}): ''{}''小於0".format(reading_name,reading_type_code,value_str))

                                case "boolean":
                                    pattern = r"^(true|false|TRUE|FALSE|True|False)$"
                                    if not re.match(pattern, value_str):
                                        # read_err_msg_map_temp["E3007"] = "E3007 {}({}): '{}'不是合法的{}格式".format(reading_name,reading_type_code,value_str,reading_data_type)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23007",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue
                                    true_list = ["true", "TRUE", "True"]
                                    if value_str in true_list:
                                        value = True
                                    else:
                                        value = False

                                case "double":
                                    value_str_split = value_str.split(".")

                                    pattern = r"^[-+]?\d*\.?\d*$"
                                    if not re.match(pattern, value_str):
                                        # read_err_msg_map_temp["E3007"] = "E3007 {}({}): '{}'不是合法的{}格式".format(reading_name,reading_type_code,value_str,reading_data_type)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23007",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue  # ignore

                                    # 檢查是否逾可接受的最大值
                                    temp = re.sub("^0*", "", value_str_split[0])  # 去除開頭的'0'

                                    if len(max_double_str) < len(temp):
                                        # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_double_str)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23008",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue
                                    elif len(max_double_str) == len(temp):
                                        if max_double_str < temp:
                                            # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_double_str)
                                            error_log.append(
                                                {
                                                    "file_type": file_type,
                                                    "file_path": file_path,
                                                    "raw_gzfile": raw_gzfile,
                                                    "raw_file": file_name,
                                                    "rec_time": rec_time,
                                                    "source": source,
                                                    "read_group": reading_type_group,
                                                    "meter_id": meter_unique_id,
                                                    "read_time": read_date_time,
                                                    "type_cd": "E23008",
                                                    "col_nm": reading_name,
                                                    "log_upd_time": datetime.now(),
                                                }
                                            )
                                            error_cnt += 1
                                            continue
                                    # 檢查是否為負值
                                    value = float(value_str)
                                    # if 0 > value:
                                    #     print("{}({}): ''{}''小於0".format(reading_name,reading_type_code,value_str))

                                case "float":
                                    value_str_split = value_str.split(".")

                                    pattern = r"^[-+]?\d*\.?\d*$"
                                    if not re.match(pattern, value_str):
                                        # read_err_msg_map_temp["E3007"] = "E3007 {}({}): '{}'不是合法的{}格式".format(reading_name,reading_type_code,value_str,reading_data_type)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23007",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue  # ignore

                                    # 檢查是否逾可接受的最大值
                                    temp = re.sub("^0*", "", value_str_split[0])  # 去除開頭的'0'
                                    # print(len(max_float_str))
                                    if len(max_float_str) < len(temp):
                                        # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_double)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23008",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue
                                    elif len(max_float_str) == len(temp):
                                        if max_float_str < temp:
                                            # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_float_str)
                                            error_log.append(
                                                {
                                                    "file_type": file_type,
                                                    "file_path": file_path,
                                                    "raw_gzfile": raw_gzfile,
                                                    "raw_file": file_name,
                                                    "rec_time": rec_time,
                                                    "source": source,
                                                    "read_group": reading_type_group,
                                                    "meter_id": meter_unique_id,
                                                    "read_time": read_date_time,
                                                    "type_cd": "E23008",
                                                    "col_nm": reading_name,
                                                    "log_upd_time": datetime.now(),
                                                }
                                            )
                                            error_cnt += 1
                                            continue
                                    # 檢查是否為負值
                                    value = float(value_str)
                                    # if 0 > value:
                                    #     if reading_name not in ['INT_PF','DEL_AVG_PF','REC_AVG_PF','PHASE_A_PF','PHASE_B_PF','PHASE_C_PF']:
                                    #         print("通訊商{} {}({}): ''{}''小於0".format(source,reading_name,reading_type_code,value_str))

                                case "integer":
                                    value_str_split = value_str.split(".")
                                    # 檢查中華一期整數送小數問題(僅顯示不跳出)
                                    # if len(value_str_split)>1:
                                    #     print("{}({}): '{}'不是合法的{}格式({})".format(reading_name,reading_type_code,value_str,reading_data_type,source))

                                    # 檢查是否為數字型態
                                    pattern = r"^(\+|-)?\d+$"
                                    if not re.match(pattern, value_str_split[0]):
                                        # read_err_msg_map_temp["E3007"] = "E3007 {}({}): '{}'不是合法的{}格式".format(reading_name,reading_type_code,value_str,reading_data_type)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23007",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue

                                    # 檢查是否逾可接受的最大值
                                    temp = re.sub("^0*", "", value_str_split[0])  # 去除開頭的'0'
                                    if len(max_int) < len(temp):
                                        # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_int)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23008",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue

                                    elif len(max_int.split(".")[0]) == len(temp):
                                        if max_int < temp:
                                            # read_err_msg_map_temp["E3008"] = "E3008 {}({}): ''{}''超過{}格式可接受最大值'{}'".format(reading_name,reading_type_code,value_str,reading_data_type,max_int)
                                            error_log.append(
                                                {
                                                    "file_type": file_type,
                                                    "file_path": file_path,
                                                    "raw_gzfile": raw_gzfile,
                                                    "raw_file": file_name,
                                                    "rec_time": rec_time,
                                                    "source": source,
                                                    "read_group": reading_type_group,
                                                    "meter_id": meter_unique_id,
                                                    "read_time": read_date_time,
                                                    "type_cd": "E23008",
                                                    "col_nm": reading_name,
                                                    "log_upd_time": datetime.now(),
                                                }
                                            )
                                            error_cnt += 1
                                            continue

                                    # 檢查是否為負值
                                    value = value_str_split[0]  # int類型給予字串避免dataframe轉型為float
                                    # if 0 > value:
                                    #     print("{}({}): ''{}''小於0".format(reading_name,reading_type_code,value_str))

                                case "timestamp":  # HES回傳為"None"轉成null, (eip轉為"1900-01-01T00:00:00.000+08:00")
                                    # if 'None' == value_str:
                                    #     value = "1900-01-01T00:00:00.000+08:00"
                                    #     continue
                                    value_str_split = value_str.split("+")  # default

                                    # 中華一在ResetTime加上時區 戴課程式：1>len(value_str_split) 需確認
                                    # if len(value_str_split) > 1 and reading_type_code == '11.22.103.0.0.2.122.0.0.0.0.0.0.0.0.0.108.0':
                                    #     print("{}({}): ''{}''不是合法的ISO8601格式({})".format(reading_name,reading_type_code,value_str,source))
                                    # reset_time(11.22.103.0.0.2.122.0.0.0.0.0.0.0.0.0.108.0): ''2022-12-23T00:00:00.000+08:00''不是合法的ISO8601格式(HES-CHT20190919)

                                    pattern = r"^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$"
                                    if not re.match(pattern, value_str_split[0]):
                                        # read_err_msg_map_temp["E3007"] = "E3007 {}({}): ''{}''不是合法的ISO8601格式".format(reading_name,reading_type_code,value_str)
                                        error_log.append(
                                            {
                                                "file_type": file_type,
                                                "file_path": file_path,
                                                "raw_gzfile": raw_gzfile,
                                                "raw_file": file_name,
                                                "rec_time": rec_time,
                                                "source": source,
                                                "read_group": reading_type_group,
                                                "meter_id": meter_unique_id,
                                                "read_time": read_date_time,
                                                "type_cd": "E23007",
                                                "col_nm": reading_name,
                                                "log_upd_time": datetime.now(),
                                            }
                                        )
                                        error_cnt += 1
                                        continue

                                    value = value_str_split[0]

                                case "varchar":
                                    # ID1, ID2不得超過8碼
                                    if reading_name in ["ID2", "ID1"]:
                                        if len(value_str) > 8:
                                            # read_err_msg_map_temp["E3010"] = "E3010 {}({})讀表值: ''{}''不合規範".format(reading_name,reading_type_code,value_str)
                                            error_log.append(
                                                {
                                                    "file_type": file_type,
                                                    "file_path": file_path,
                                                    "raw_gzfile": raw_gzfile,
                                                    "raw_file": file_name,
                                                    "rec_time": rec_time,
                                                    "source": source,
                                                    "read_group": reading_type_group,
                                                    "meter_id": meter_unique_id,
                                                    "read_time": read_date_time,
                                                    "type_cd": "E23010",
                                                    "col_nm": reading_name,
                                                    "log_upd_time": datetime.now(),
                                                }
                                            )
                                            error_cnt += 1
                                            continue
                                        else:
                                            value = value_str
                                    elif "TOU_ID" == reading_name:
                                        pattern = r"^\d{1,6}$"
                                        if re.match(pattern, value_str):
                                            value = value_str
                                        else:
                                            # read_err_msg_map_temp["E3010"] = "E3010 {}({})讀表值: ''{}''不合規範".format(reading_name,reading_type_code,value_str)
                                            error_log.append(
                                                {
                                                    "file_type": file_type,
                                                    "file_path": file_path,
                                                    "raw_gzfile": raw_gzfile,
                                                    "raw_file": file_name,
                                                    "rec_time": rec_time,
                                                    "source": source,
                                                    "read_group": reading_type_group,
                                                    "meter_id": meter_unique_id,
                                                    "read_time": read_date_time,
                                                    "type_cd": "E23010",
                                                    "col_nm": reading_name,
                                                    "log_upd_time": datetime.now(),
                                                }
                                            )
                                            error_cnt += 1
                                            continue
                                    else:
                                        value = value_str
                        except Exception as ex:
                            # read_err_msg_map_temp["E3009"] = "E3009 {}({})讀表值: ''{}''轉換為{}格式發生例外: '{}'".format(reading_name,reading_type_code,value_str,reading_data_type,ex.getMessage())
                            error_log.append(
                                {
                                    "file_type": file_type,
                                    "file_path": file_path,
                                    "raw_gzfile": raw_gzfile,
                                    "raw_file": file_name,
                                    "rec_time": rec_time,
                                    "source": source,
                                    "read_group": reading_type_group,
                                    "meter_id": meter_unique_id,
                                    "read_time": read_date_time,
                                    "type_cd": "E23009",
                                    "col_nm": reading_name,
                                    "log_upd_time": datetime.now(),
                                }
                            )
                            error_cnt += 1
                            continue

                    # hash
                    read_key = (
                        f"{reading_type_group}|{meter_unique_id}|{record_no}|{read_date_time}"
                    )

                    # 處理讀表資料

                    # ---------------------------------------------------------------------------- #
                    #                                     ALERT                                    #
                    # ---------------------------------------------------------------------------- #

                    if read_key not in read_map:
                        read_map[read_key] = {}
                        read_map[read_key].update(header_map)  # Header
                        read_map[read_key].update(
                            {
                                "meter_id": meter_unique_id,
                                "meter_mrid": mRID,
                                "read_time": read_date_time,
                                "interval": interval_state,
                                "note": interval_state_note,
                                "rec_no": record_no,
                                "hash": read_key,
                                "rt_count": 0,
                            }
                        )  # Payload初始化資訊
                        # process_cnt += 1

                    # 儲存讀表值
                    reading_name = reading_name.lower()
                    read_map[read_key][reading_name] = value  # reading value

                    # 計算HES所回傳的ReadingtType種類
                    read_map[read_key]["rt_count"] += 1

        df_read = pd.DataFrame.from_dict(read_map, orient="index")

        # end script
        # dedup_cnt = total_cnt - process_cnt
        # log_end_time = datetime.now()
        # log_time = str(log_end_time - log_start_time).split(' ')[-1]

        # Info Log
        file_info.update(
            {
                "file_type": file_type,
                "file_path": file_path,
                "raw_gzfile": raw_gzfile,
                "raw_file": file_name,
                "rec_time": rec_time,
                "msg_id": message_id,
                "corr_id": correlation_id,
                "msg_time": msg_time,
                "source": source,
                "read_group": reading_type_group,
                "total_cnt": total_cnt,
                "err_cnt": error_cnt,
                "log_upd_time": datetime.now(),
                "status": "1",
            }
        )

        df_error_log = pd.DataFrame(error_log)
        df_info_log = pd.DataFrame([file_info])

        return df_read, df_error_log, df_info_log

    except etree.XMLSyntaxError as e:
        # print("XML Syntax Error: ", e.error_log.last_error)

        # process_cnt += 1

        # end script
        # log_end_time = datetime.now()
        # log_time = log_end_time - log_start_time

        # Erorr Log
        error_log.append(
            {
                "file_type": file_type,
                "file_path": file_path,
                "raw_gzfile": raw_gzfile,
                "raw_file": file_name,
                "rec_time": rec_time,
                "source": None,
                "read_group": None,
                "meter_id": None,
                "read_time": None,
                "type_cd": "E20001",
                "col_nm": None,
                "log_upd_time": datetime.now(),
            }
        )
        error_cnt += 1
        # Info Log
        file_info.update(
            {
                "file_type": file_type,
                "file_path": file_path,
                "raw_gzfile": raw_gzfile,
                "raw_file": file_name,
                "rec_time": rec_time,
                "msg_id": None,
                "corr_id": None,
                "msg_time": None,
                "source": None,
                "read_group": None,
                "total_cnt": None,
                "err_cnt": error_cnt,
                "log_upd_time": datetime.now(),
                "status": "0",
            }
        )
        df_read = pd.DataFrame()
        df_error_log = pd.DataFrame(error_log)
        df_info_log = pd.DataFrame([file_info])

        return df_read, df_error_log, df_info_log

    else:  # 非["LP", "MN", "ALT", "DR"]
        # process_cnt += 1

        # end script
        # log_end_time = datetime.now()
        # log_time = log_end_time - log_start_time

        # Erorr Log
        error_log.append(
            {
                "file_type": file_type,
                "file_path": file_path,
                "raw_gzfile": raw_gzfile,
                "raw_file": file_name,
                "rec_time": None,
                "source": None,
                "read_group": None,
                "meter_id": None,
                "read_time": None,
                "type_cd": "E20001",
                "col_nm": None,
                "log_upd_time": datetime.now(),
            }
        )
        error_cnt += 1
        # Info Log
        file_info.update(
            {
                "file_type": file_type,
                "file_path": file_path,
                "raw_gzfile": raw_gzfile,
                "raw_file": file_name,
                "rec_time": None,
                "msg_id": None,
                "corr_id": None,
                "msg_time": None,
                "source": None,
                "read_group": None,
                "total_cnt": None,
                "err_cnt": error_cnt,
                "log_upd_time": datetime.now(),
                "status": "0",
            }
        )
        df_read = pd.DataFrame()
        df_error_log = pd.DataFrame(error_log)
        df_info_log = pd.DataFrame([file_info])

        return df_read, df_error_log, df_info_log


def process_df_read(df_read, file_group):
    # 調整columns順序&填補無ReadingType的欄位
    column_list = []
    match file_group:
        case "LP":
            column_list = [
                "file_type",
                "file_path",
                "file_size",
                "raw_gzfile",
                "raw_file",
                "rec_time",
                "hash",
                "rt_count",
                "msg_id",
                "corr_id",
                "msg_time",
                "verb",
                "noun",
                "context",
                "source",
                "rev",
                "qos",
                "rtgrp",
                "meter_id",
                "meter_mrid",
                "rec_no",
                "read_time",
                "interval",
                "note",
                "del_kwh",
                "rec_kwh",
                "del_kvarh_lag",
                "del_kvarh_lead",
                "rec_kvarh_lag",
                "rec_kvarh_lead",
            ]

        case "MN" | "DR":
            column_list = [
                "file_type",
                "file_path",
                "file_size",
                "raw_gzfile",
                "raw_file",
                "rec_time",
                "hash",
                "rt_count",
                "msg_id",
                "corr_id",
                "msg_time",
                "verb",
                "noun",
                "context",
                "source",
                "rev",
                "qos",
                "rtgrp",
                "meter_id",
                "meter_mrid",
                "rec_no",
                "read_time",
                "interval",
                "note",
                "battery_time",
                "tou_id",
                "reset_time",
                "del_max_kw_time",
                "ctpt_ratio",
                "ct_ratio",
                "pt_ratio",
                "del_kwh",
                "del_rate_a_kwh",
                "del_rate_a_max_kw",
                "del_rate_a_cum_kw",
                "del_rate_a_cont_kw",
                "del_rate_b_kwh",
                "del_rate_b_max_kw",
                "del_rate_b_cum_kw",
                "del_rate_b_cont_kw",
                "del_rate_c_kwh",
                "del_rate_c_max_kw",
                "del_rate_c_cum_kw",
                "del_rate_c_cont_kw",
                "del_rate_d_kwh",
                "del_rate_d_max_kw",
                "del_rate_d_cum_kw",
                "del_rate_d_cont_kw",
                "reset_count",
                "del_lag_kvarh",
                "del_lead_kvarh",
                "rec_lag_kvarh",
                "rec_lead_kvarh",
                "prog_date",
                "phase_n_curr_over",
                "rec_max_kw_time",
                "rec_kwh",
                "rec_rate_a_kwh",
                "rec_rate_a_max_kw",
                "rec_rate_a_cum_kw",
                "rec_rate_a_cont_kw",
                "rec_rate_b_kwh",
                "rec_rate_b_max_kw",
                "rec_rate_b_cum_kw",
                "rec_rate_b_cont_kw",
                "rec_rate_c_kwh",
                "rec_rate_c_max_kw",
                "rec_rate_c_cum_kw",
                "rec_rate_c_cont_kw",
                "rec_rate_d_kwh",
                "rec_rate_d_max_kw",
                "rec_rate_d_cum_kw",
                "rec_rate_d_cont_kw",
                "del_rate_a_max_kw_time",
                "del_rate_b_max_kw_time",
                "del_rate_c_max_kw_time",
                "del_rate_d_max_kw_time",
                "rec_inst_kw",
                "rec_rate_e_kwh",
                "rec_rate_e_max_kw",
                "rec_rate_e_cum_kw",
                "rec_rate_e_cont_kw",
                "rec_rate_f_kwh",
                "rec_rate_f_max_kw",
                "rec_rate_f_cum_kw",
                "rec_rate_f_cont_kw",
                "del_rate_e_kwh",
                "del_rate_e_max_kw",
                "del_rate_e_cum_kw",
                "del_rate_e_cont_kw",
                "del_rate_f_kwh",
                "del_rate_f_max_kw",
                "del_rate_f_cum_kw",
                "del_rate_f_cont_kw",
                "del_rate_e_max_kw_time",
                "del_rate_f_max_kw_time",
                "demand_remain_sec",
                "inst_kva",
                "inst_kw",
                "int_pf",
                "id2",
                "id1",
                "cust_id",
            ]

        case "ALT":
            column_list = [
                "file_type",
                "file_path",
                "file_size",
                "raw_gzfile",
                "raw_file",
                "rec_time",
                "hash",
                "rt_count",
                "msg_id",
                "corr_id",
                "msg_time",
                "verb",
                "noun",
                "context",
                "source",
                "rev",
                "qos",
                "rtgrp",
                "meter_id",
                "meter_mrid",
                "rec_no",
                "read_time",
                "interval",
                "note",
                "inst_kva",
                "inst_kw",
                "int_pf",
                "phase_a_volt",
                "phase_b_volt",
                "phase_c_volt",
                "phase_a_curr",
                "phase_b_curr",
                "phase_c_curr",
                "phase_n_curr",
                "phase_a_volt_angle",
                "phase_b_volt_angle",
                "phase_c_volt_angle",
                "phase_a_curr_angle",
                "phase_b_curr_angle",
                "phase_c_curr_angle",
                "del_avg_pf",
                "line_ab_volt",
                "line_ab_curr",
                "line_ab_volt_angle",
                "line_ab_curr_angle",
                "rec_avg_pf",
                "hz",
                "phase_a_pf",
                "phase_b_pf",
                "phase_c_pf",
                "rec_inst_kw",
            ]

    # reorder columns
    df_read = df_read.reindex(columns=column_list)

    # 避免Dataframe中null值顯示NaN
    df_read = df_read.replace(np.nan, None)

    # cals cust_id when file_group in ('MN','DR')
    if file_group in ["MN", "DR"]:
        # 電號
        custid_lookup_table = {
            "00": 0,
            "01": 1,
            "02": 2,
            "03": 3,
            "04": 4,
            "05": 5,
            "06": 6,
            "07": 7,
            "08": 8,
            "09": 9,
            "10": 2,
            "11": 3,
            "12": 4,
            "13": 5,
            "14": 6,
            "15": 7,
            "16": 8,
            "17": 9,
            "18": 0,
            "19": 1,
            "20": 4,
            "21": 5,
            "22": 6,
            "23": 7,
            "24": 8,
            "25": 9,
            "26": 0,
            "27": 1,
            "28": 2,
            "29": 3,
            "30": 6,
            "31": 7,
            "32": 8,
            "33": 9,
            "34": 0,
            "35": 1,
            "36": 2,
            "37": 3,
            "38": 4,
            "39": 5,
            "40": 8,
            "41": 9,
            "42": 0,
            "43": 1,
            "44": 2,
            "45": 3,
            "46": 4,
            "47": 5,
            "48": 6,
            "49": 7,
            "50": 1,
            "51": 2,
            "52": 3,
            "53": 4,
            "54": 5,
            "55": 6,
            "56": 7,
            "57": 8,
            "58": 9,
            "59": 0,
            "60": 3,
            "61": 4,
            "62": 5,
            "63": 6,
            "64": 7,
            "65": 8,
            "66": 9,
            "67": 0,
            "68": 1,
            "69": 2,
            "70": 5,
            "71": 6,
            "72": 7,
            "73": 8,
            "74": 9,
            "75": 0,
            "76": 1,
            "77": 2,
            "78": 3,
            "79": 4,
            "80": 7,
            "81": 8,
            "82": 9,
            "83": 0,
            "84": 1,
            "85": 2,
            "86": 3,
            "87": 4,
            "88": 5,
            "89": 6,
            "90": 9,
            "91": 0,
            "92": 1,
            "93": 2,
            "94": 3,
            "95": 4,
            "96": 5,
            "97": 6,
            "98": 7,
            "99": 8,
        }

        # define ratio column
        df_read["ratio"] = pd.Series([], dtype=object)

        for index in df_read.index:
            id1 = df_read.loc[index]["id1"]
            id2 = df_read.loc[index]["id2"]

            if id1 != None and id2 != None:
                pattern = r"^[0-9]{8}"
                if re.match(pattern, id1) and re.match(pattern, id2):
                    cust_id10 = id2[-2:] + id1
                    chk = (
                        custid_lookup_table[cust_id10[0:2]]
                        + custid_lookup_table[cust_id10[2:4]]
                        + custid_lookup_table[cust_id10[4:6]]
                        + custid_lookup_table[cust_id10[6:8]]
                        + custid_lookup_table[cust_id10[8:10]]
                    )

                    cust_id11 = cust_id10 + str(chk)[-1:]
                    df_read.loc[index, "cust_id"] = cust_id11

            ctpt = (
                None
                if df_read.loc[index]["ctpt_ratio"] == None
                else int(df_read.loc[index, "ctpt_ratio"])
            )
            pt = "" if df_read.loc[index]["pt_ratio"] == None else df_read.loc[index]["pt_ratio"]
            ct = "" if df_read.loc[index]["ct_ratio"] == None else df_read.loc[index]["ct_ratio"]
            if ctpt != None:
                df_read.loc[index, "ratio"] = ctpt
            elif pt == "" or ct == "":
                df_read.loc[index, "ratio"] = None
            else:
                df_read.loc[index, "ratio"] = int(pt) * int(ct)

    return df_read
