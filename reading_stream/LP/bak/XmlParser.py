#!/usr/bin/ python3
"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) P6 XML 解析
  (2) 格式檢查及轉換
"""
# Author：OOO
# Date：OOOO/OO/OO
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import sys

sys.path.append("/usr/lib/python3/dist-packages")
sys.path.append("../../reading_stream")
import gzip
import json
from datetime import datetime

import gzinfo
import pandas as pd
import xmltodict
from ami import constant, func, lp_config
from confluent_kafka import Producer

if __name__ == "__main__":
    exitcode = 0

    file = sys.argv[1]
    try:
        # delimeter = "\\"
        delimeter = "/"
        part = file.split(delimeter)
        path = delimeter.join(part[0:-1])
        file_name = part[-1]
        file_type = file_name.split(".")[1].lower()

        vTime = None
        rec_time = None
        opener = open
        # get GZ original filename
        if file_type == "gz":
            info = gzinfo.read_gz_info(file)
            file_name = info.fname
            opener = gzip.open

        # get file timestamp
        vTime = file_name.split(".")[0].split("_")[2]
        rec_time = (
            vTime[0:4]
            + "-"
            + vTime[4:6]
            + "-"
            + vTime[6:8]
            + "T"
            + vTime[8:10]
            + ":"
            + vTime[10:12]
            + ":"
            + vTime[12:14]
        )

        # parse xml file
        with opener(file, "rt") as fd:
            doc = xmltodict.parse(fd.read())
            # HEADER INFO.
            qos = doc["EventMessage"]["Header"]["Property"][1]["Value"]
            source = doc["EventMessage"]["Header"]["Source"]
            msg_id = doc["EventMessage"]["Header"]["MessageID"]
            corr_id = doc["EventMessage"]["Header"]["CorrelationID"]
            msg_time = doc["EventMessage"]["Header"]["Timestamp"][0:19].replace("T", " ")
            rev = doc["EventMessage"]["Header"]["Revision"]
            qos = doc["EventMessage"]["Header"]["Property"][1]["Value"]

            # Getting the current date and time
            log_start_time = datetime.now()
            dt = log_start_time.strftime("%Y-%m-%d %H:%M:%S")

            # Counter
            meters = {}
            errors = 0
            totals = 0

            # PAYLOAD INFO.
            for items in doc["EventMessage"]["Payload"]["MeterReadings"]["MeterReading"]:
                meter_id = None
                read_time = None
                rec_no = None

                if items["Meter"]["Names"]["NameType"]["name"] == "MeterUniqueID":
                    meter_id = items["Meter"]["Names"]["name"]
                    if meter_id not in meters.keys():
                        meters[meter_id] = 1

                columns = {}
                rt_count = 0
                interval = 0
                note = 0

                # METER讀表值
                for readings in items["IntervalBlocks"]:
                    rt = None
                    read_time = readings["IntervalReadings"]["timeStamp"]
                    if read_time != None:
                        read_time = read_time[0:19]
                    read_val = readings["IntervalReadings"]["value"]

                    # 依據ReadingQuality判斷是否為rec_no或者讀表狀態
                    for code in readings["IntervalReadings"]["ReadingQualities"]:
                        if code["ReadingQualityType"]["@ref"] == "5.4.260":
                            rec_no = code["comment"]
                        if code["ReadingQualityType"]["@ref"] != "5.4.260":
                            ref_code = code["ReadingQualityType"]["@ref"]
                            interval = constant.qualitycode[ref_code]["interval"]
                            note = constant.qualitycode[ref_code]["note"]

                    # 讀值欄位
                    rt = constant.loadprofile[readings["ReadingType"]["@ref"]]["name"]
                    columns[rt] = read_val
                    if rt != None:
                        rt_count += 1

                meters[meter_id] += 1
                part_no = func.hash_func(meter_id, 4)
                # topic_nm = 'lp-raw'

                del_kwh = columns["del_kwh"] if "del_kwh" in columns else None
                rec_kwh = columns["rec_kwh"] if "rec_kwh" in columns else None
                del_kvarh_lag = columns["del_kvarh_lag"] if "del_kvarh_lag" in columns else None
                del_kvarh_lead = columns["del_kvarh_lead"] if "del_kvarh_lead" in columns else None
                rec_kvarh_lag = columns["rec_kvarh_lag"] if "rec_kvarh_lag" in columns else None
                rec_kvarh_lead = columns["rec_kvarh_lead"] if "rec_kvarh_lead" in columns else None

                # LP 讀表物件
                tmp = {
                    "source": source,
                    "meter_id": meter_id,
                    "fan_id": "",
                    "rec_no": rec_no,
                    "read_time": read_time,
                    "interval": interval,
                    "note": note,
                    "del_kwh": del_kwh,
                    "rec_kwh": rec_kwh,
                    "del_kvarh_lag": del_kvarh_lag,
                    "del_kvarh_lead": del_kvarh_lead,
                    "rec_kvarh_lag": rec_kvarh_lag,
                    "rec_kvarh_lead": rec_kvarh_lead,
                    "version": 1,
                    "proc_type": 0,
                    "file_type": file_type,
                    "raw_gzfile": "",
                    "raw_file": file,
                    "rec_time": rec_time,
                    "file_path": path,
                    "file_size": "1789431",
                    "file_seqno": "1",
                    "msg_id": msg_id,
                    "corr_id": corr_id,
                    "msg_time": msg_time,
                    "rev": rev,
                    "qos": qos,
                    "rt_count": rt_count,
                }
                totals += 1

                # validation
                try:
                    # 測試用
                    # tmp = {'source':source, 'meter_id': meter_id, 'fan_id': '', 'rec_no':rec_no, 'read_time': read_time, 'interval':interval, 'note':note, 'del_kwh': del_kwh, 'rec_kwh': rec_kwh, 'del_kvarh_lag': del_kvarh_lag, 'del_kvarh_lead': del_kvarh_lead, 'rec_kvarh_lag': rec_kvarh_lag, 'rec_kvarh_lead': rec_kvarh_lead, 'version': 1, 'proc_type': 0, 'file_type':file_type, 'raw_gzfile': '', 'raw_file': file, 'rec_time': rec_time, 'file_path': path, 'file_size': '1789431', 'file_seqno': '1', 'msg_id': msg_id, 'corr_id': corr_id, 'msg_time': msg_time, 'rev': rev, 'qos': qos, 'rt_count': rt_count}
                    lp_config.LP(**tmp)

                except lp_config.ValidationError as e:
                    for v in e.errors():
                        warn_type = constant.warn_cd[v["type"]]
                        column = v["loc"][0]
                        o_val = tmp[column]
                        new_val = constant.warn_cd[v["type"]]["func"](o_val)
                        # 格式修訂
                        tmp[column] = new_val

                        # 格式錯誤 | 資料異常告警 Log
                        warn_log = {
                            "file_type": file_type,
                            "raw_gzfile": "",
                            "raw_file": file,
                            "rec_time": rec_time,
                            "file_path": path,
                            "source": tmp["source"],
                            "read_group": "LP",
                            "meter_id": meter_id,
                            "read_time": read_time,
                            "type_cd": warn_type,
                            "col_nm": column,
                            "o_val": o_val,
                        }
                        # print("warn_log: ",warn_log)

                topics = json.dumps(tmp)
                print(topics)
                # break

    except Exception as e:
        print("2", e)
        exitcode = 1
    finally:
        print("Duration: {}".format(datetime.now() - log_start_time))
        sys.exit(exitcode)
