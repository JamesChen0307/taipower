import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from os.path import dirname

import numpy as np
import pandas as pd
from lxml import etree

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + '/../'))
from datetime import datetime

# from ..ami import constant, func, lp_config
from ami.constant import LOADPROFILE, QUALITYCODE

# from ami.constant import LOADPROFILE
# from ami import constant, func, lp_config


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

    file_group_name, file_uuid, *_ = file_name.split("_")

    # stored 讀表資料
    header_tmp = {}
    read_map = {}

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
        verb = root.find(".//ns:Verb", ns).text
        noun = root.find(".//ns:Noun", ns).text
        context = root.find(".//ns:Context", ns).text
        rev = root.find(".//ns:Revision", ns).text

        for item in root.findall(".//ns:Property", ns):
            name = item.find("ns:Name", ns).text
            value = item.find("ns:Value", ns).text
            # 檢查ReadingTypeGroup
            if name == "ReadingTypeGroup":
                read_group = value

            # 檢查QOSLevel
            if name == "QOSLevel":
                qos = value
            # 檢查MessageIndex
            if name == "MessageIndex":
                msg_idx = value

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
            "qos": qos,
        }

        print(header_tmp)

        # ---------------------------------------------------------------------------- #
        #                               Get Payload Info                               #
        # ---------------------------------------------------------------------------- #
        mRID = ""
        meter_unique_id = ""
        fan_unique_id = ""
        read_date_time = ""  # 讀表值產生時間 ISO8601格式
        interval_state = 1  # 間距狀態代碼(1:normal, 2:short, 3:long, 4:invalid); default=1
        interval_state_note = 1  # 間距狀態原因註記代碼(1:無, 2:time changed校時, 3:power down斷電, 4:fault電表故障, 5:work施工有電, 6:power-down施工無電, 7:手工輸入); default=1
        record_no = None

        # ------------------------------ 1.Meter Readings ----------------------------- #
        meter_readings_list = root.find(".//ns2:MeterReadings", ns2)
        for meter_reading in meter_readings_list.findall("ns2:MeterReading", ns2):
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

            # ------------------------------ 2.Interval Blocks ----------------------------- #
            interval_blocks_list = meter_reading.findall("ns2:IntervalBlocks", ns2)
            for intervalBlock in interval_blocks_list:
                # read_err_msg_map_temp = {} #暫存當筆Reading的檢查錯誤 [type:message, ...]
                reading_type_code = ""

                _reading_type_code = intervalBlock.find("ns2:ReadingType", ns2).attrib["ref"]
                if _reading_type_code in LOADPROFILE:
                    reading_type_code = _reading_type_code

                # ---------------------------- 3.Interval Readings ---------------------------- #
                interval_readings_list = intervalBlock.findall("ns2:IntervalReadings", ns2)
                for interval_reading in interval_readings_list:
                    # 處理Readtime
                    try:
                        read_date_time = interval_reading.find("ns2:timeStamp", ns2).text
                        datetime.strptime(read_date_time, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        print()

                    # --------------------------- 4.Reading Qualities --------------------------- #
                    reading_quality_list = interval_reading.findall("ns2:ReadingQualities", ns2)
                    for read_quality in reading_quality_list:
                        if read_quality.find("ns2:ReadingQualityType", ns2).attrib["ref"] == "5.4.260":
                            record_no = read_quality.find('ns2:comment', ns2)
                        if read_quality.find("ns2:ReadingQualityType", ns2).attrib["ref"] == "1.5.257":
                            # ------------------------------ data_error_log ------------------------------ #
                            print('')
                        if read_quality.find("ns2:ReadingQualityType", ns2).attrib["ref"] != "5.4.260":
                            ref_code = read_quality.find("ns2:ReadingQualityType", ns2).attrib["ref"]
                            interval_state = QUALITYCODE[ref_code]["interval"]
                            interval_state_note = QUALITYCODE[ref_code]["note"]

                    # 轉換讀表值為適當的資料格式
                    value = None
                    reading_name = ""
                    reading_data_type = None

                    if reading_type_code != None:
                        lookup_map = LOADPROFILE[reading_type_code]
                        reading_name = lookup_map["name"]
                        reading_data_type = lookup_map["type"]

                        if (
                            interval_reading.find("ns2:value", ns2) == None
                            or interval_reading.find("ns2:value", ns2).text == None
                        ):
                            if reading_data_type == "timestamp":
                                value = "1900-01-01T00:00:00.000+08:00"  # HES回傳為"None"轉成null, (eip轉為"1900-01-01T00:00:00.000+08:00")
                            continue
                        else:
                            value_str = interval_reading.find(
                                "ns2:value", ns2
                            ).text.strip()  # 原始讀表值


                    # hash
                    read_key = (
                        f"{read_group}|{meter_unique_id}|{record_no}|{read_date_time}"
                    )

                    # 處理讀表資料

                    # ---------------------------------------------------------------------------- #
                    #                                     ALERT                                    #
                    # ---------------------------------------------------------------------------- #

                    if read_key not in read_map:
                        read_map[read_key] = {}
                        read_map[read_key].update(header_tmp)  # Header
                        read_map[read_key].update(
                            {
                                "meter_id": meter_unique_id,
                                "fan_id": fan_unique_id,
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

        return df_read
    except etree.XMLSyntaxError as e:
        print("XML Syntax Error: ", e.error_log.last_error)


print(parse_xml(
    "C:/Users/zian0/Desktop/Omni/Taipower/documents/SDS交付文件/mdes/reading-stream/LP/LP_1cbd57c9-1ddc-42c2-bb22-07da17f6ad92_20221118084235.xml",
    "LP_1cbd57c9-1ddc-42c2-bb22-07da17f6ad92_20221118084235.xml",
))
