"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 處理XML檔案
"""

# Author：JamesChen
# Date：2023/O5/22
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import logging
import os
import sys
import multiprocessing as mp
from datetime import datetime
from os.path import dirname

import pytz
import redis
import xmltodict

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + "/../"))

from ami import conn, func, lp_config, constant

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
    TARGET_TIMEZONE = pytz.timezone("Asia/Taipei")

    # ---------------------------------------------------------------------------- #
    #                              File Log Attributes                             #
    # ---------------------------------------------------------------------------- #

    # 讀取Flowfile Attributes
    file_type = sys.argv[1]
    file_path = sys.argv[2]
    file_size = sys.argv[3]
    rec_time = sys.argv[4] + " " + sys.argv[5]
    raw_file = sys.argv[6]
    raw_gzfile = sys.argv[7]

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
    #                                   Main Data                                  #
    # ---------------------------------------------------------------------------- #
    MAIN_ENABLE_MK = 0

    # ---------------------------------------------------------------------------- #
    #                               Multi Processing                               #
    # ---------------------------------------------------------------------------- #
    cpus = mp.cpu_count()
    mppool = mp.Pool(processes=cpus)

    meters = {}

    try:
        # 讀取Flowfile內容
        data = sys.stdin.buffer.read().decode("utf-8")

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
        file_seqno = 1

        filelog_key = "filelog:" + varFileKey + "_" + str(file_seqno)
        print(filelog_key)
        # ---------------------------------------------------------------------------- #
        #                                  XML Parsing                                 #
        # ---------------------------------------------------------------------------- #

        try:
            log_start_time = datetime.now().strftime(DATE_FORMAT)
            doc = xmltodict.parse(data)
            print(doc)
            # ---------------------------------------------------------------------------- #
            #                                Get Header Info                               #
            # ---------------------------------------------------------------------------- #
            source = doc["EventMessage"]["Header"]["Source"]
            msg_id = doc["EventMessage"]["Header"]["MessageID"]
            corr_id = doc["EventMessage"]["Header"]["CorrelationID"]
            msg_time = doc["EventMessage"]["Header"]["Timestamp"]
            verb = doc["EventMessage"]["Header"]["Verb"]
            noun = doc["EventMessage"]["Header"]["Noun"]
            context = doc["EventMessage"]["Header"]["Context"]
            rev = doc["EventMessage"]["Header"]["Revision"]
            read_group = doc["EventMessage"]["Header"]["Property"][0]["Value"]
            qos = doc["EventMessage"]["Header"]["Property"][1]["Value"]
            msg_idx = doc["EventMessage"]["Header"]["Property"][2]["Value"]


            if msg_time is not None:
                msg_time = datetime.strptime(msg_time, "%Y-%m-%dT%H:%M:%S.%f%z").strftime(DATE_FORMAT)
                print(msg_time)
            else:
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
                    msg_id=msg_id,
                    source=source,
                    read_group=read_group,
                    total_cnt=0,
                    warn_cnt=0,
                    main_succ_cnt=0,
                    dedup_cnt=0,
                    err_cnt=0,
                    dup_cnt=0,
                    hist_cnt=0,
                    wait_cnt=0,
                    fnsh_cnt=0,
                    proc_type=10,
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
            if qos not in ["LEVEL-1", "LEVEL-2", "LEVEL-3"]:
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
                    msg_id=msg_id,
                    source=source,
                    read_group=read_group,
                    total_cnt=0,
                    warn_cnt=0,
                    main_succ_cnt=0,
                    dedup_cnt=0,
                    err_cnt=0,
                    dup_cnt=0,
                    hist_cnt=0,
                    wait_cnt=0,
                    fnsh_cnt=0,
                    proc_type=11,
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
            if source not in constant.SOURCE_WHITELIST:
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
                    msg_id=msg_id,
                    source=source,
                    read_group=read_group,
                    total_cnt=0,
                    warn_cnt=0,
                    main_succ_cnt=0,
                    dedup_cnt=0,
                    err_cnt=0,
                    dup_cnt=0,
                    hist_cnt=0,
                    wait_cnt=0,
                    fnsh_cnt=0,
                    proc_type=12,
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

            header_dict = {
                "source": source,
                "msg_id": msg_id,
                "corr_id": corr_id,
                "msg_time": msg_time,
                "verb": verb,
                "noun": noun,
                "context": context,
                "rev": rev,
                "read_group": read_group,
                "qos": qos,
                "msg_idx": msg_idx,
            }
            print(header_dict)

            results = []

            for meter_reading in doc["EventMessage"]["Payload"]["MeterReadings"]["MeterReading"]:
                match meter_reading["Meter"]["Names"]["NameType"]["name"]:
                    case "MeterUniqueID":
                        meter_id = meter_reading["Meter"]["Names"]["name"]
                    case "FanUniqueID":
                        fan_id = meter_reading["Meter"]["Names"]["name"]
                print("here")
                result = mppool.apply_async(
                    func.get_payload,
                    (
                        flowfile_attr,
                        file_seqno,
                        file_dir_date,
                        header_dict,
                        meter_reading,
                        meters
                    ),
                )
                results.append(result)
            mppool.close()
            mppool.join()

            # print(results)
            # outputs = [result.get() for result in results]
            # print(outputs)

            payload_results = []
            for result in results:
                try:
                    result_value = result.get(timeout=10)
                    payload_results.append(result_value)
                except TimeoutError:
                    print("獲取結果超時")
                except Exception as e:
                    print("獲取結果超時發生異常:", str(e))
            total_cnt, warn_cnt, err_cnt, wait_cnt, main_succ_cnt = func.count_total(payload_results)

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
                msg_id=msg_id,
                source=source,
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
                proc_type=9,
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
                msg_id=msg_id,
                source=source,
                read_group=read_group,
                total_cnt=total_cnt,
                warn_cnt=warn_cnt,
                main_succ_cnt=main_succ_cnt,
                dedup_cnt=0,
                err_cnt=err_cnt,
                dup_cnt=0,
                hist_cnt=0,
                wait_cnt=warn_cnt,
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
            # print("Duration: {}".format(datetime.now() - start_time))
            sys.exit(exitcode)

    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_XMLExtract",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
