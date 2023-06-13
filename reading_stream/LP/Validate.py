"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) Load Profile 讀表資料檢核作業
"""

# Author：JamesChen
# Date：2023/O5/29
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import json
import logging
import pytz
import os
import sys
from datetime import date, datetime, timedelta
from os.path import dirname

import redis

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + "/../"))
from ami import conn, func

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
    #                                  Kafka Topic                                 #
    # ---------------------------------------------------------------------------- #

    warn_log_topic = "mdes.stream.data-warn-log"
    error_log_topic = "mdes.stream.data-error-log"
    lp_raw_topic = "mdes.stream.lp-raw"
    lpi_raw_topic = "mdes.stream.lpi-raw"
    lpr_hist = "mdes.stream.lpr-hist"
    lpi_hist = "mdes.stream.lpi-hist"

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
    err_cnt = 0

    try:
        # ---------------------------------------------------------------------------- #
        #                               Flowfile Content                               #
        # ---------------------------------------------------------------------------- #

        # 讀取Flowfile內容
        flowfile_json = sys.stdin.buffer.read().decode("utf-8")
        flowfile_data = json.loads(flowfile_json)

        read_time = flowfile_data["read_time"]
        read_time_int = int(datetime.strptime(flowfile_data["read_time"], DATE_FORMAT).timestamp())
        read_date = datetime.strptime(flowfile_data["read_time"], DATE_FORMAT).strftime("%Y-%m-%d")
        meter_id = flowfile_data["meter_id"]
        meter_type = flowfile_data["meter_id"][:2]
        rec_no = flowfile_data.get("rec_no", None)
        comment = flowfile_data.get("comment", None)
        interval = flowfile_data["interval"]
        note = flowfile_data["note"]
        source = flowfile_data["source"]
        read_group = flowfile_data["read_group"]
        hist_mk = flowfile_data.get("hsit_mk", 0)
        hist_dur_ts = flowfile_data["hist_dur_ts"]
        file_batch_no = flowfile_data["file_batch_no"]
        batch_mk = flowfile_data["batch_mk"]
        raw_file = flowfile_data["raw_file"]
        file_seqno = flowfile_data["file_seqno"]
        rt_count = flowfile_data["rt_count"]
        del_kwh = flowfile_data["del_kwh"]
        msg_time = flowfile_data.get("msg_time", None)
        rec_time = flowfile_data.get("rec_time", None)

        flowfile_attr = {
            "file_type": flowfile_data["file_type"],
            "file_path": flowfile_data["file_path"],
            "file_size": flowfile_data["file_size"],
            "rec_time": flowfile_data["rec_time"],
            "raw_file": flowfile_data["raw_file"],
            "raw_gzfile": flowfile_data["raw_gzfile"],
        }

        # -------------------------- varFileKey, varSrchKey -------------------------- #
        varFileKey = raw_file.rsplit(".", 1)[0]  # 去除副檔名
        varSrchKey = varFileKey.replace("-", "\\-") + "*"
        filelog_key = "filelog:" + varFileKey + "_" + str(file_seqno)

        # 從 Redis JSON 中獲取 proc_type 的值
        proc_type = redis_conn.execute_command("JSON.GET", "filelog_key", ".proc_type")

        # 檢查 proc_type 是否小於 4
        if proc_type and int(proc_type) < 4:
            # 更新 Redis JSON 中的 proc_type 為 4
            redis_conn.execute_command("JSON.SET", "filelog_key", ".proc_type", "4")

        if file_batch_no and batch_mk == 2:
            # 跳過下述檢查
            pass
        elif hist_mk != 1 and flowfile_data["read_time"] <= datetime.now() - timedelta(days=7):
            hist_start_time = datetime.now()
            hist_mk = 1
            flowfile_data["src_flow"] = "LP_Validate"
            # 將資料拋轉至Kafka mdes.stream.lpr-hist或mdes.stream.lpi-hist
            if flowfile_data["source"] != "HES-TMAP20210525":
                func.publish_kafka(flowfile_data, lpr_hist, func.hash_func(meter_id))
            else:
                func.publish_kafka(flowfile_data, lpi_hist, func.hash_func(meter_id))
            # 設定 hist_cnt+1
            hist_cnt = redis_conn.execute_command("JSON.GET", "filelog_key", ".hist_cnt") + 1
            # 更新 filelog.hist_cnt+1
            redis_conn.execute_command("JSON.SET", "filelog_key", ".hist_cnt", hist_cnt)
            # 結束本處理程序
        else:
            # 其他情況的處理邏輯
            pass

        # 從 Redis JSON 中獲取 hist_mk 的值
        hist_mk = redis_conn.execute_command("JSON.GET", "filelog_key", ".hist_mk")
        if hist_mk and int(hist_mk) == 1:
            # 更新對應filelog.hist_cnt-1
            hist_mk -= 1
            redis_conn.execute_command("JSON.SET", "filelog_key", ".hist_mk", hist_mk)

        # ---------------------------------------------------------------------------- #
        #                                     去重檢查                                   #
        # ---------------------------------------------------------------------------- #
        dedup_start_time = datetime.now()
        dedup_result = redis_conn.execute_command(
            "JSON.GET",
            "lp_data:" + meter_id + "_" + read_date,
            '$.data[?(@.read_time_int=={0}&&@.meter_id=="{1}"&&@.rec_no=={2}&&@.interval=={3}&&@.note=={4})]'.format(
                read_time_int, meter_id, rec_no, interval, note
            ),
        )

        dupstat_key = "dup_stat:" + read_date + "_" + meter_id
        if dedup_result is not None:  # 有重複資料
            if redis_conn.exists(dupstat_key):
                log_dup = redis_conn.execute_command(
                    "JSON.GET", dupstat_key, "$.data[?(@.read_time_int=={0}]".format(read_time_int)
                )
                if log_dup is not None:  # 已經有資料在dup_log裡 將dup_cnt+1
                    redis_conn.execute_command(
                        "JSON.NUMINCRBY",
                        dupstat_key,
                        "$.data[?(@.read_time_int=={0})].dup_cnt 1".format(read_time_int),
                    )
                    redis_conn.execute_command(
                        "JSON.SET",
                        dupstat_key,
                        "$.data[?(@.read_time_int=={0})].dup_upd_time".format(read_time_int),
                        json.dumps(datetime.now().strftime(DATE_FORMAT)),
                    )

                else:
                    new_dup_data = {
                        "source": source,
                        "read_group": read_group,
                        "meter_type": meter_type,
                        "meter_id": meter_id,
                        "read_time": read_date,
                        "read_time_int": read_time_int,
                        "read_time_bias": func.calculate_read_time_bias(read_time),
                        "dup_cnt": 1,
                        "log_start_time": datetime.now(),
                        "log_upd_time": "",
                        "log_end_time": "",
                    }
                    new_dup_json = json.dumps(new_dup_data)
                    redis_conn.execute_command(
                        "JSON.ARRAPPEND", dupstat_key, "$.data", new_dup_json
                    )

            else:
                dup_stat_data = {
                    "log_date_int": int(datetime.now().timestamp()),
                    "data": [
                        {
                            "source": source,
                            "read_group": read_group,
                            "meter_type": meter_type,
                            "meter_id": meter_id,
                            "read_time": read_date,
                            "read_time_int": read_time_int,
                            "read_time_bias": func.calculate_read_time_bias(read_time),
                            "dup_cnt": 1,
                            "log_start_time": datetime.now(),
                            "log_upd_time": "",
                            "log_end_time": "",
                        }
                    ],
                }
                dup_stat_json = json.dumps(dup_stat_data)
                redis_conn.execute_command("JSON.SET", dupstat_key, ".", dup_stat_json)
                redis_conn.execute_command("EXPIRE", dupstat_key, conn.MDES_REDIS_TTL)
            dedup_dur_ts = str(datetime.now() - dedup_start_time)
            redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".dedup_cnt", 1)
            exitcode = 1
            sys.exit(exitcode)
        else:
            # ---------------------------------------------------------------------------- #
            #                                    資料檢核作業                                #
            # ---------------------------------------------------------------------------- #
            error_start_time = datetime.now()
            # ----------------------------- 讀表時間(read_time)無值 ---------------------------- #
            if read_time is None:
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20001",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            # -------------------------- 讀表時間(read_time)不符合區間規範 -------------------------- #
            if datetime.strptime(read_time, DATE_FORMAT).minute % 15 != 0:
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20002",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            # ------------------------ 讀表時間(read_time)為未來日期(大於隔日) ------------------------ #
            if datetime.strptime(read_time, DATE_FORMAT).date() > date.today() + timedelta(days=1):
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20003",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            # ------------------------------- LP 讀值項目總計不合規範 ------------------------------ #
            if rt_count != 2 or rt_count != 6:
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20004",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            # ---------------------------- 區間售電仟瓦小時(del_kwh)無值 --------------------------- #
            if del_kwh is None:
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20005",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            # ---------------------------------- 電表表號無值 ---------------------------------- #
            if meter_id is None:
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20006",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            # --------------------------------- 早於AMI建置時間 -------------------------------- #
            if datetime.strptime(read_time, DATE_FORMAT) < datetime.strptime(
                "2018-01-01 00:00:00", DATE_FORMAT
            ):
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20007",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            # -------------------------- TMAP 讀表 comment值非 1 ~ 4 ------------------------- #
            if (
                source == "HES-TMAP20210525"
                and interval == 1
                and note == 1
                and comment not in ["0", "1", "2", "3", "4"]
            ):
                func.publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    source,
                    read_group,
                    meter_id,
                    read_time,
                    "E20012",
                )
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".err_cnt", 1)
                exitcode = 1
                sys.exit(exitcode)
            error_dur_ts = str(datetime.now() - error_start_time)

            # ---------------------------------------------------------------------------- #
            #                                   有無異常表號重複                              #
            # ---------------------------------------------------------------------------- #
            dup_start_time = datetime.now()

            dup_result = redis_conn.execute_command(
                "JSON.GET",
                "lp_data:" + meter_id + "_" + read_date,
                '$.data[?(@.read_time_int=={0}&&@.meter_id=="{1}"&&@.rec_no!={2}&&@.del_kwh!={3})]'.format(
                    read_time_int, meter_id, rec_no, del_kwh
                ),
            )

            lpdupmeter_key = "lp_dup_meter:" + meter_id + "_" + read_date
            if len(json.loads(dup_result.decode())) > 1:
                log_data = {
                    "source": source,
                    "meter_type": meter_type,
                    "meter_id": meter_id,
                    "read_time": read_date,
                    "read_time_bias": func.calculate_read_time_bias(read_time),
                    "data_no": 1,
                    "rec_no": rec_no,
                    "del_kwh": del_kwh,
                    "interval": interval,
                    "note": note,
                    "log_date_time": datetime.now(),
                }
                log_data_json = json.dumps(log_data)
                if redis_conn.exists(lpdupmeter_key):
                    lp_dup_meter_result = redis_conn.execute_command(
                        "JSON.GET",
                        lpdupmeter_key,
                        "$.data[?(@.read_time_int=={0})]".format(read_time_int),
                    )
                    if len(json.loads(lp_dup_meter_result.decode())) == 0:
                        redis_conn.execute_command(
                            "JSON.ARRAPPEND", lpdupmeter_key, "$.data", log_data_json
                        )
                    else:
                        log_data["data_no"] = len(json.loads(lp_dup_meter_result.decode())) + 1
                        log_data_json = json.dumps(log_data)
                        redis_conn.execute_command(
                            "JSON.ARRAPPEND", lpdupmeter_key, "$.data", log_data_json
                        )
                else:
                    lp_dup_meter_data = {
                        "log_date_int": int(datetime.now().timestamp()),
                        "data": [
                            {
                                "source": source,
                                "meter_type": meter_type,
                                "meter_id": meter_id,
                                "read_time": read_date,
                                "read_time_bias": func.calculate_read_time_bias(read_time),
                                "data_no": 1,
                                "rec_no": rec_no,
                                "del_kwh": del_kwh,
                                "interval": interval,
                                "note": note,
                                "log_date_time": datetime.now(),
                            }
                        ],
                    }
                    lp_dup_meter_json = json.dumps(lp_dup_meter_data)
                    redis_conn.execute_command("JSON.SET", lpdupmeter_key, ".", lp_dup_meter_json)
                    redis_conn.execute_command("EXPIRE", lpdupmeter_key, conn.MDES_REDIS_TTL)
                redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".dup_cnt", 1)
            dup_dur_ts = str(datetime.now() - dup_start_time)

            # ---------------------------------------------------------------------------- #
            #                                     實際值更新                                 #
            # ---------------------------------------------------------------------------- #
            ver_start_time = datetime.now()

            lpdata_key = "lp_data:" + meter_id + "_" + read_date
            lpidata_key = "lpi_data:" + meter_id + "_" + read_date

            if source == "HES-TMAP20210525":
                if hist_mk != 1:
                    hist_mk = 0
                    flowfile_data["hist_dur_ts"] = 0

                lpi_result = redis_conn.execute_command(
                    "JSON.GET",
                    lpidata_key,
                    '$.data[?(@.read_time_int=={0}&&@.meter_id=="{1}")]'.format(
                        read_time_int, meter_id
                    ),
                )

                if len(json.loads(lpi_result.decode())) > 1:
                    lpi_comment = json.loads(lpi_result.decode())["comment"]
                    if comment > lpi_comment:
                        func.publish_kafka(
                            json.loads(lpi_result.decode()),
                            "mdes.stream.lpi_ver",
                            func.hash_func(meter_id),
                        )
                        flowfile_data["dup_dur_ts"] = dup_dur_ts
                        flowfile_data["error_dur_ts"] = error_dur_ts
                        flowfile_data["ver_dur_ts"] = str(datetime.now() - ver_start_time)
                        flowfile_data["end_strm_time"] = datetime.now().strftime(DATE_FORMAT)
                        flowfile_data["version"] += 1
                        lpi_json = json.dumps(flowfile_data)
                        redis_conn.execute_command("JSON.SET", lpidata_key, ".", lpi_json)
                        func.publish_kafka(
                            flowfile_data,
                            "mdes.stream.lpi",
                            func.hash_func(meter_id),
                        )
                        redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".fnsh_cnt", 1)
                else:
                    if redis_conn.exists(lpidata_key):
                        flowfile_data["dup_dur_ts"] = dup_dur_ts
                        flowfile_data["error_dur_ts"] = error_dur_ts
                        flowfile_data["ver_dur_ts"] = str(datetime.now() - ver_start_time)
                        flowfile_data["end_strm_time"] = datetime.now().strftime(DATE_FORMAT)
                        lpi_json = json.dumps(flowfile_data)

                        redis_conn.execute_command("JSON.ARRAPPEND", lpidata_key, ".data", lpi_json)
                    else:
                        lpi_data = {
                            "meter_id": meter_id,
                            "read_date_int": read_time_int,
                            "data": [],
                        }
                        lpidata_json = json.dumps(lpi_data)

                        flowfile_data["dup_dur_ts"] = dup_dur_ts
                        flowfile_data["error_dur_ts"] = error_dur_ts
                        flowfile_data["ver_dur_ts"] = str(datetime.now() - ver_start_time)
                        flowfile_data["end_strm_time"] = datetime.now().strftime(DATE_FORMAT)
                        lpi_json = json.dumps(flowfile_data)

                        redis_conn.execute_command("JSON.SET", lpidata_key, ".", lpidata_json)
                        redis_conn.execute_command("EXPIRE", lpidata_key, conn.MDES_REDIS_TTL)
                        redis_conn.execute_command("JSON.ARRAPPEND", lpidata_key, ".data", lpi_json)
                        func.publish_kafka(
                            flowfile_data,
                            "mdes.stream.lpi",
                            func.hash_func(meter_id),
                        )
                        redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".fnsh_cnt", 1)
                func.publish_kafka(
                    flowfile_data,
                    "mdes.stream.lpi-preprocessed",
                    func.hash_func(meter_id),
                )
            else:
                if hist_mk != 1:
                    hist_mk = 0
                    flowfile_data["hist_dur_ts"] = 0

                lp_result = redis_conn.execute_command(
                    "JSON.GET",
                    lpdata_key,
                    '$.data[?(@.read_time_int=={0}&&@.meter_id=="{1}")]'.format(
                        read_time_int, meter_id
                    ),
                )

                if len(json.loads(lp_result.decode())) > 1:
                    lp_msg_time = json.loads(lp_result.decode())["msg_time"]
                    lp_rec_time = json.loads(lp_result.decode())["rec_time"]
                    if msg_time > lp_msg_time or rec_time > lp_rec_time:
                        func.publish_kafka(
                            json.loads(lp_result.decode()),
                            "mdes.stream.lpr_ver",
                            func.hash_func(meter_id),
                        )
                        flowfile_data["dup_dur_ts"] = dup_dur_ts
                        flowfile_data["error_dur_ts"] = error_dur_ts
                        flowfile_data["ver_dur_ts"] = str(datetime.now() - ver_start_time)
                        flowfile_data["end_strm_time"] = datetime.now().strftime(DATE_FORMAT)
                        flowfile_data["version"] += 1
                        lp_json = json.dumps(flowfile_data)
                        redis_conn.execute_command("JSON.SET", lpdata_key, ".", lp_json)
                        func.publish_kafka(
                            flowfile_data,
                            "mdes.stream.lpr",
                            func.hash_func(meter_id),
                        )
                        redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".fnsh_cnt", 1)
                else:
                    if redis_conn.exists(lpdata_key):
                        flowfile_data["dup_dur_ts"] = dup_dur_ts
                        flowfile_data["error_dur_ts"] = error_dur_ts
                        flowfile_data["ver_dur_ts"] = str(datetime.now() - ver_start_time)
                        flowfile_data["end_strm_time"] = datetime.now().strftime(DATE_FORMAT)
                        flowfile_data["msg_time_int"] = int(
                            datetime.strptime(flowfile_data["msg_time"], DATE_FORMAT).timestamp()
                        )
                        flowfile_data["rec_time_int"] = int(
                            datetime.strptime(flowfile_data["rec_time"], DATE_FORMAT).timestamp()
                        )
                        lp_json = json.dumps(flowfile_data)

                        redis_conn.execute_command("JSON.ARRAPPEND", lpdata_key, ".data", lp_json)
                    else:
                        lp_data = {
                            "meter_id": meter_id,
                            "read_date_int": read_time_int,
                            "data": [],
                        }
                        lpdata_json = json.dumps(lp_data)

                        flowfile_data["dup_dur_ts"] = dup_dur_ts
                        flowfile_data["error_dur_ts"] = error_dur_ts
                        flowfile_data["ver_dur_ts"] = str(datetime.now() - ver_start_time)
                        flowfile_data["end_strm_time"] = datetime.now().strftime(DATE_FORMAT)
                        flowfile_data["msg_time_int"] = int(
                            datetime.strptime(flowfile_data["msg_time"], DATE_FORMAT).timestamp()
                        )
                        flowfile_data["rec_time_int"] = int(
                            datetime.strptime(flowfile_data["rec_time"], DATE_FORMAT).timestamp()
                        )
                        lp_json = json.dumps(flowfile_data)

                        redis_conn.execute_command("JSON.SET", lpdata_key, ".", lpdata_json)
                        redis_conn.execute_command("EXPIRE", lpdata_key, conn.MDES_REDIS_TTL)
                        redis_conn.execute_command("JSON.ARRAPPEND", lpdata_key, ".data", lp_json)
                        func.publish_kafka(
                            flowfile_data,
                            "mdes.stream.lpr",
                            func.hash_func(meter_id),
                        )
                        redis_conn.execute_command("JSON.NUMINCRBY", filelog_key, ".fnsh_cnt", 1)
                func.publish_kafka(
                    flowfile_data,
                    "mdes.stream.lp-preprocessed",
                    func.hash_func(meter_id),
                )
            filelog_result = redis_conn.execute_command("JSON.GET", filelog_key)
            filelog = json.loads(filelog_result.decode())
            if (
                filelog["wait_cnt"] == 0
                and filelog["hist_cnt"] == 0
                and filelog["total_cnt"]
                == filelog["dedup_cnt"] + filelog["err_cnt"] + filelog["err_cnt"]
                and filelog["log_end_time"] == datetime.now()
            ):
                filelog["proc_type"] = 6
                filelog["log_end_time"] == datetime.now()
                func.publish_kafka(
                    filelog,
                    "mdes.stream.file-log",
                    func.hash_func(meter_id),
                )
                redis_conn.delete(filelog_key)
            else:
                update_filelog = {"proc_type": 5, "log_upd_time": datetime.now()}
                redis_conn.execute_command("JSON.SET", filelog_key, ".", json.dumps(update_filelog))

    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_Validate",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
