"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 處理CSV檔案
"""

# Author：JamesChen
# Date：2023/O5/25
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import csv
# import pytz
import logging
import os
import sys
from datetime import datetime
from io import StringIO
from os.path import dirname

import redis

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + "/../"))

from ami import conn, constant, func, lp_config


def _rt_count_fields(input_data):
    fields_to_check = [
        "DEL_KWH",
        "REC_KWH",
        "DEL_KVARH_LAG",
        "DEL_KVARH_LEAD",
        "REC_KVARH_LAG",
        "REC_KVARH_LEAD",
    ]
    return sum(1 for field in fields_to_check if field in input_data and input_data[field])


if __name__ == "__main__":
    exitcode = 0

    # ---------------------------------------------------------------------------- #
    #                                    Format                                    #
    # ---------------------------------------------------------------------------- #

    # date format config
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

    # log format config
    LOGGING_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
    logging.basicConfig(level=logging.ERROR, format=LOGGING_FORMAT, datefmt=DATE_FORMAT)

    # 設定目標時區為 GMT+08:00
    # TARGET_TIMEZONE = pytz.timezone('Asia/Taipei')

    # ---------------------------------------------------------------------------- #
    #                              File Log Attributes                             #
    # ---------------------------------------------------------------------------- #

    # 讀取Flowfile Attributes
    file_type = sys.argv[1]
    file_path = sys.argv[2]
    file_size = sys.argv[3]
    rec_time = sys.argv[4]
    raw_file = sys.argv[5]
    raw_gzfile = sys.argv[6]

    flowfile_attr = {
        "file_type": file_type,
        "file_path": file_path,
        "file_size": file_size,
        "rec_time": rec_time,
        "raw_file": raw_file,
        "raw_gzfile": raw_gzfile,
    }

    # ------------------------ file_dir_ym, file_dir_date ------------------------ #
    # 從字串中提取日期部分
    date_part = file_path.split("/")[-3:]  # 從後往前取最後三個元素
    date_string = "/".join(date_part)  # 將元素重新組合成字串，形如 "2023/05/19"

    # 解析日期字串為日期物件
    date = datetime.strptime(date_string, "%Y/%m/%d")
    file_dir_date = date.strftime("%Y-%m-%d")
    # 提取年份和月份，並組合為新的變數
    file_dir_ym = datetime.strftime(date, "%Y-%m-01")

    # -------------------------------- read_group -------------------------------- #
    read_group = raw_file.split("_")[1]

    # -------------------------- varFileKey, varSrchKey -------------------------- #
    varFileKey = raw_file.rsplit(".", 1)[0]  # 去除副檔名
    varSrchKey = varFileKey.replace("-", "\\-") + "*"

    # ---------------------------------------------------------------------------- #
    #                                  Kafka Topic                                 #
    # ---------------------------------------------------------------------------- #

    warn_log_topic = "mdes.stream.data-warn-log"
    error_log_topic = "mdes.stream.data-error-log"
    lp_raw_topic = "mdes.stream.lp-raw"
    lpi_raw_topic = "mdes.stream.lpi-raw"

    # ---------------------------------------------------------------------------- #
    #                               Redis Connection                               #
    # ---------------------------------------------------------------------------- #
    pool = redis.ConnectionPool(
        host=conn.MDES_REDIS_HOST,
        port=conn.MDES_REDIS_PORT,
        password=conn.MDES_REDIS_PASS,
        db=conn.MDES_REDIS_DB,
    )
    redis_conn = redis.Redis(connection_pool=pool)

    # ---------------------------------------------------------------------------- #
    #                                    Counts                                    #
    # ---------------------------------------------------------------------------- #
    total_cnt = 0
    warn_cnt = 0
    err_cnt = 0
    wait_cnt = 0
    main_succ_cnt = 0

    # ---------------------------------------------------------------------------- #
    #                                  Dictionary                                  #
    # ---------------------------------------------------------------------------- #
    lp_raw_temp = {}
    lpi_raw_temp = {}

    # ---------------------------------------------------------------------------- #
    #                                   Main Data                                  #
    # ---------------------------------------------------------------------------- #
    MAIN_ENABLE_MK = 0

    try:
        # 讀取Flowfile內容
        data = sys.stdin.buffer.read().decode("utf-8")
        # 移除 \ufeff 字元
        data = data.replace('\ufeff', '')

        srch_obj = redis_conn.execute_command(
            "ft.aggregate",
            "filelog_idx",
            f"@raw_file:{{{varSrchKey}}}",
            "GROUPBY",
            "1",
            "@raw_file",
            "REDUCE",
            "MAX",
            "1",
            "@seqno",
            "as",
            "seqno",
        )

        # 檢查srch_obj列表的長度是否大於1
        if len(srch_obj) > 1:
            o = func.convert_redislist(srch_obj[1])
            max_seqno = o["seqno"]
            print("filelog max seqno:", max_seqno)
            file_seqno = int(max_seqno) + 1
        else:
            # print("No result returned from ft.aggregate")
            file_seqno = 1

        filelog_key = "filelog:" + varFileKey + "_" + str(file_seqno)

        # ---------------------------------------------------------------------------- #
        #                                  CSV Parsing                                 #
        # ---------------------------------------------------------------------------- #
        try:
            start_time = datetime.now()
            log_start_time = str(start_time)[0:19]

            csv_start_time = datetime.now()
            csv_file = StringIO(data.strip())  # 加上 .strip() 去除起始和結束的換行字符

            # ------------------------------- Header Check ------------------------------- #
            header_reader = csv.reader(csv_file)
            row = next(header_reader)

            # CSV欄位數或欄位項目不符規範
            if row != constant.CSV_HEADER:
                file_log = lp_config.FileLog(
                    log_date_int=int(datetime.now().timestamp()),
                    file_type=file_type,
                    raw_gzfile=raw_gzfile,
                    raw_file=raw_file,
                    rec_time=rec_time,
                    file_path=file_path,
                    file_dir_ym=file_dir_ym,
                    file_dir_date=file_dir_date,
                    file_seqno=file_seqno,
                    msg_id="",
                    source="",
                    read_group="",
                    total_cnt=0,
                    warn_cnt=0,
                    main_succ_cnt=0,
                    dedup_cnt=0,
                    err_cnt=0,
                    dup_cnt=0,
                    hist_cnt=0,
                    wait_cnt=0,
                    fnsh_cnt=0,
                    proc_type=13,
                    file_batch_no="",
                    batch_mk="",
                    log_start_time=log_start_time,
                    log_upd_time="",
                    log_end_time="",
                    dw_update_time="",
                )
                func.set_redis(redis_conn, filelog_key, file_log)
                exitcode = 1
                sys.exit(exitcode)
            # --------------------------------- CSV Value -------------------------------- #
            csv_file.seek(0)  # 將檔案指標歸零
            value_reader = csv.DictReader(csv_file)
            data_list = list(value_reader)
            for reading_data in enumerate(data_list):
                start_strm_time = str(datetime.now())[0:19]
                rt_count = _rt_count_fields(reading_data[1])
                # --------------------------------- 通訊商代號不合規範 -------------------------------- #
                if reading_data[1]["SOURCE"] not in constant.SOURCE_WHITELIST:
                    func.publish_errorlog(
                        flowfile_attr,
                        file_seqno,
                        reading_data[1]["SOURCE"],
                        read_group,
                        reading_data[1]["METER_ID"],
                        reading_data[1]["READ_TIME"],
                        "E20014",
                    )
                    err_cnt += 1
                    exitcode = 1
                    sys.exit(exitcode)
                # ----------------------------- CSV讀表Record欄位數量不符 ---------------------------- #
                if any(value is None for value in reading_data[1].values()):
                    func.publish_errorlog(
                        flowfile_attr,
                        file_seqno,
                        reading_data[1]["SOURCE"],
                        read_group,
                        reading_data[1]["METER_ID"],
                        reading_data[1]["READ_TIME"],
                        "E20013",
                    )
                    err_cnt += 1
                    exitcode = 1
                    sys.exit(exitcode)

                if reading_data[1]["SOURCE"] == "HES-TMAP20210525":
                    lpi_raw_temp = {
                        "source": reading_data[1]["SOURCE"],
                        "meter_id": reading_data[1]["METER_ID"],
                        "fan_id": reading_data[1]["FAN_ID"],
                        "tamp_cust_id": reading_data[1]["CUST_ID"],
                        "comment": reading_data[1]["REC_NO"],
                        "read_time": reading_data[1]["READ_TIME"][0:19].replace("T", " "),
                        "interval": reading_data[1]["INTERVAL"],
                        "note": reading_data[1]["NOTE"],
                        "del_kwh": reading_data[1]["DEL_KWH"],
                        "rec_kwh": reading_data[1]["REC_KWH"],
                        "del_kvarh_lag": reading_data[1]["DEL_KVARH_LAG"],
                        "del_kvarh_lead": reading_data[1]["DEL_KVARH_LEAD"],
                        "rec_kvarh_lag": reading_data[1]["REC_KVARH_LAG"],
                        "rec_kvarh_lead": reading_data[1]["REC_KVARH_LEAD"],
                        "sdp_id": "",
                        "ratio": "",
                        "pwr_co_id": "",
                        "cust_id": "",
                        "ct_ratio": "",
                        "pt_ratio": "",
                        "file_type": file_type,
                        "raw_gzfile": raw_gzfile,
                        "raw_file": raw_file,
                        "rec_time": rec_time,
                        "file_path": file_path,
                        "file_size": file_size,
                        "file_seqno": file_seqno,
                        "msg_id": "",
                        "corr_id": "",
                        "msg_time": "",
                        "read_group": read_group,
                        "verb": "",
                        "noun": "",
                        "context": "",
                        "msg_idx": "",
                        "rev": "",
                        "qos": "",
                        "start_strm_time": start_strm_time,
                        "warn_dur_ts": "",
                        "main_dur_ts": "",
                        "rt_count": rt_count,
                    }
                    # print(lpi_raw_temp)
                    csv_dur_ts = datetime.now() - csv_start_time
                    check_start_time = datetime.now()
                    lpi_raw_temp = func.check_data(
                        lpi_raw_temp, flowfile_attr, file_seqno, read_group, rt_count, warn_cnt
                    )
                    check_dur_ts = datetime.now() - check_start_time
                    # print(lpi_raw_temp)
                else:
                    lp_raw_temp = {
                        "source": reading_data[1]["SOURCE"],
                        "meter_id": reading_data[1]["METER_ID"],
                        "fan_id": reading_data[1]["FAN_ID"],
                        "tamp_cust_id": reading_data[1]["CUST_ID"],
                        "rec_no": reading_data[1]["REC_NO"],
                        "read_time": reading_data[1]["READ_TIME"][0:19].replace("T", " "),
                        "interval": reading_data[1]["INTERVAL"],
                        "note": reading_data[1]["NOTE"],
                        "del_kwh": reading_data[1]["DEL_KWH"],
                        "rec_kwh": reading_data[1]["REC_KWH"],
                        "del_kvarh_lag": reading_data[1]["DEL_KVARH_LAG"],
                        "del_kvarh_lead": reading_data[1]["DEL_KVARH_LEAD"],
                        "rec_kvarh_lag": reading_data[1]["REC_KVARH_LAG"],
                        "rec_kvarh_lead": reading_data[1]["REC_KVARH_LEAD"],
                        "sdp_id": "",
                        "ratio": "",
                        "pwr_co_id": "",
                        "cust_id": "",
                        "ct_ratio": "",
                        "pt_ratio": "",
                        "file_type": file_type,
                        "raw_gzfile": raw_gzfile,
                        "raw_file": raw_file,
                        "rec_time": rec_time,
                        "file_path": file_path,
                        "file_size": file_size,
                        "file_seqno": file_seqno,
                        "msg_id": "",
                        "corr_id": "",
                        "msg_time": "",
                        "read_group": read_group,
                        "verb": "",
                        "noun": "",
                        "context": "",
                        "msg_idx": "",
                        "rev": "",
                        "qos": "",
                        "start_strm_time": start_strm_time,
                        "warn_dur_ts": "",
                        "main_dur_ts": "",
                        "rt_count": rt_count,
                    }
                    # print(lp_raw_temp)
                    lp_raw_temp = func.check_data(
                        lp_raw_temp, flowfile_attr, file_seqno, read_group, rt_count, warn_cnt
                    )
                    # print(lp_raw_temp)
                total_cnt += 1

                # ---------------------------------------------------------------------------- #
                #                             lp_raw with main_data                            #
                # ---------------------------------------------------------------------------- #
                main_start_time = datetime.now()
                read_time = reading_data[1]["READ_TIME"][0:19].replace("T", " ")
                meter_id = reading_data[1]["METER_ID"]
                source = reading_data[1]["SOURCE"]
                read_time_ux = int(
                    datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp()
                )

                if MAIN_ENABLE_MK == 1:
                    main_srch = redis_conn.execute_command(
                        "ft.search",
                        "maindata_idx",
                        f"@meter:{{{meter_id}}}",
                        "RETURN",
                        "2",
                        "$.sdp_id",
                        f"$.data[?(@.begin<={read_time_ux}&&@.end>{read_time_ux})]",
                    )

                    if (
                        main_srch[0] == 1
                    ):  # 假設有主檔 會回傳一個list包含[1(主檔比數), maindata_idx, [$.sdp_id,$.date]]
                        read_time_int = int(
                            datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp()
                        )
                        if source == "HES-TMAP20210525":
                            func.combine_maindata(main_srch, lpi_raw_temp, main_start_time)
                            main_succ_cnt += 1
                            func.publish_kafka(
                                lpi_raw_temp,
                                "mdes.stream.lpi-raw",
                                func.hash_func(meter_id),
                            )
                        else:
                            func.combine_maindata(main_srch, lp_raw_temp, main_start_time)
                            main_succ_cnt += 1
                            func.publish_kafka(
                                lp_raw_temp,
                                "mdes.stream.lp-raw",
                                func.hash_func(meter_id),
                            )
                    else:
                        wait_cnt += 1
                        if main_srch[0] > 1:
                            func.publish_errorlog(
                                flowfile_attr,
                                file_seqno,
                                source,
                                read_group,
                                meter_id,
                                read_time,
                                "E23003",
                            )
                            warn_cnt += 1
                        main_dur_ts = str(datetime.now()-main_start_time)
                        read_time_int = int(
                            datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp()
                        )

                        if source == "HES-TMAP20210525":
                            func.set_nomaindata(
                                lpi_raw_temp,
                                main_dur_ts,
                                read_time_int,
                                meter_id,
                                file_dir_date,
                                redis_conn,
                            )
                        else:
                            func.set_nomaindata(
                                lp_raw_temp,
                                main_dur_ts,
                                read_time_int,
                                meter_id,
                                file_dir_date,
                                redis_conn,
                            )
                else:
                    if source == "HES-TMAP20210525":
                        kafka_start_time = datetime.now()
                        func.publish_kafka(
                            lpi_raw_temp,
                            "mdes.stream.lpi-raw",
                            func.hash_func(meter_id),
                        )
                        kafka_dur_ts = datetime.now() - kafka_start_time
                        # print(lpi_raw_temp)
                    else:
                        func.publish_kafka(
                            lp_raw_temp,
                            "mdes.stream.lp-raw",
                            func.hash_func(meter_id),
                        )
                        print(lp_raw_temp)
            total_cnt = len(data_list)
            dur_ts = {
                "Parsing": csv_dur_ts,
                "Check": check_dur_ts,
                "Kafka": kafka_dur_ts
            }
            print(dur_ts)
        except Exception as e:
            file_log = lp_config.FileLog(
                log_date_int=int(datetime.now().timestamp()),
                file_type=file_type,
                raw_gzfile=raw_gzfile,
                raw_file=raw_file,
                rec_time=rec_time,
                file_path=file_path,
                file_dir_ym=file_dir_ym,
                file_dir_date=file_dir_date,
                file_seqno=file_seqno,
                msg_id="",
                source="",
                read_group=read_group,
                total_cnt=total_cnt,
                warn_cnt=warn_cnt,
                main_succ_cnt=0,
                dedup_cnt=0,
                err_cnt=err_cnt,
                dup_cnt=0,
                hist_cnt=0,
                wait_cnt=0,
                fnsh_cnt=0,
                proc_type=14,
                file_batch_no="",
                batch_mk="",
                log_start_time=log_start_time,
                log_upd_time="",
                log_end_time="",
                dw_update_time="",
            )
            func.set_redis(redis_conn, filelog_key, file_log)
            exitcode = 1
            sys.exit(exitcode)
        finally:
            file_log = lp_config.FileLog(
                log_date_int=int(datetime.now().timestamp()),
                file_type=file_type,
                raw_gzfile=raw_gzfile,
                raw_file=raw_file,
                rec_time=rec_time,
                file_path=file_path,
                file_dir_ym=file_dir_ym,
                file_dir_date=file_dir_date,
                file_seqno=file_seqno,
                msg_id="",
                source="",
                read_group=read_group,
                total_cnt=total_cnt,
                warn_cnt=warn_cnt,
                main_succ_cnt=0,
                dedup_cnt=0,
                err_cnt=err_cnt,
                dup_cnt=0,
                hist_cnt=0,
                wait_cnt=0,
                fnsh_cnt=0,
                proc_type=1,
                file_batch_no="",
                batch_mk="",
                log_start_time=log_start_time,
                log_upd_time="",
                log_end_time="",
                dw_update_time="",
            )
            func.set_redis(redis_conn, filelog_key, file_log)
            print(filelog_key)
            print("Duration: {}".format(datetime.now() - start_time))
            sys.exit(exitcode)

    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_CSVExtract",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
