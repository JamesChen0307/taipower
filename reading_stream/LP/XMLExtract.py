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
from datetime import datetime
from os.path import dirname

import pytz
import redis
import xmltodict

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + "/../"))

from ami import conn, func, lp_config
from ami.constant import LOADPROFILE, QUALITYCODE

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
            file_seqno = max_seqno + 1
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
                msg_time = msg_time[0:19].replace("T", " ")
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
            if source not in [
                "HES-CHT20180705",
                "HES-CHT20190919",
                "HES-DAS20180705",
                "HES-FET20190919",
                "HES-UBIIK20180705",
                "HES-APTG20180628",
                "HES-TMAP20210525",
            ]:
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
            # ---------------------------------------------------------------------------- #
            #                               Get Payload Info                               #
            # ---------------------------------------------------------------------------- #

            # Paramter init
            read_time = None
            reading_type = None
            rec_no = None
            meter_id = None
            interval = 0
            note = 0

            # Counter
            meters = {}
            columns = {}
            rt_count = 0
            warn_cnt = 0
            err_cnt = 0
            main_succ_cnt = 0

            # ------------------------------ 1.Meter Readings ----------------------------- #
            for meter_reading in doc["EventMessage"]["Payload"]["MeterReadings"]["MeterReading"]:
                start_strm_time = str(datetime.now())[0:19]
                match meter_reading["Meter"]["Names"]["NameType"]["name"]:
                    case "MeterUniqueID":
                        meter_id = meter_reading["Meter"]["Names"]["name"]
                    case "FanUniqueID":
                        fan_id = meter_reading["Meter"]["Names"]["name"]
                if meter_id not in meters.keys():
                    meters[meter_id] = 1

                # ------------------------------ 2.Interval Blocks ----------------------------- #
                for interval_block in meter_reading["IntervalBlocks"]:
                    read_time = interval_block["IntervalReadings"]["timeStamp"]
                    if read_time != None:
                        read_time = read_time[0:19].replace("T", " ")
                    # ------------------------- 臨時程式ID號碼 (TOU_id) 處理台達程式 ------------------------- #
                    match interval_block["ReadingType"]["@ref"]:
                        case "0.0.0.0.0.2.167.0.0.0.0.0.0.0.0.0.0.0":
                            reading_type == "TOU_ID"
                            func.publish_warnlog(
                                flowfile_attr,
                                file_seqno,
                                source,
                                read_group,
                                meter_id,
                                read_time,
                                "W23002",
                                "reading_type",
                                rt_count,
                            )
                            warn_cnt += 1
                        case None:
                            func.publish_warnlog(
                                flowfile_attr,
                                file_seqno,
                                source,
                                read_group,
                                meter_id,
                                read_time,
                                "W24001",
                                "reading_type",
                                rt_count,
                            )
                            warn_cnt += 1
                        case _:
                            reading_type = LOADPROFILE[interval_block["ReadingType"]["@ref"]][
                                "name"
                            ]  # 讀值欄位
                            read_val = interval_block["IntervalReadings"]["value"]
                            columns[reading_type] = read_val
                            rt_count = len(columns)

                    # ---------------------------- 3.Reading Qualities --------------------------- #
                    for reading_quality in interval_block["IntervalReadings"]["ReadingQualities"]:
                        if source != "HES-TMAP20210525":
                            match reading_quality["ReadingQualityType"]["@ref"]:
                                case "5.4.260":
                                    rec_no = reading_quality[
                                        "comment"
                                    ]  # 依據Reading Quality判斷是否為rec_no或者讀表狀態
                                case "1.5.257":
                                    func.publish_errorlog(
                                        flowfile_attr,
                                        file_seqno,
                                        source,
                                        read_group,
                                        meter_id,
                                        read_time,
                                        "E21001",
                                    )
                                    err_cnt += 1
                                    exitcode = 1
                                    sys.exit(exitcode)
                                case _:
                                    ref_code = reading_quality["ReadingQualityType"]["@ref"]
                                    interval = QUALITYCODE[ref_code]["interval"]
                                    note = QUALITYCODE[ref_code]["note"]
                        else:
                            match reading_quality["ReadingQualityType"]["@ref"]:
                                case "5.4.260":
                                    comment = reading_quality[
                                        "comment"
                                    ]  # 依據Reading Quality判斷是否為rec_no或者讀表狀態
                                case "1.5.257":
                                    func.publish_errorlog(
                                        flowfile_attr,
                                        file_seqno,
                                        source,
                                        read_group,
                                        meter_id,
                                        read_time,
                                        "E21001",
                                    )
                                    err_cnt += 1
                                    exitcode = 1
                                    sys.exit(exitcode)
                                case _:
                                    ref_code = reading_quality["ReadingQualityType"]["@ref"]
                                    interval = QUALITYCODE[ref_code]["interval"]
                                    note = QUALITYCODE[ref_code]["note"]

                        read_val = interval_block["IntervalReadings"]["value"]
                        if reading_type is not None:
                            columns[reading_type] = read_val

                        del_kwh = columns["DEL_KWH"] if "DEL_KWH" in columns else None
                        rec_kwh = columns["REC_KWH"] if "REC_KWH" in columns else None
                        del_kvarh_lag = (
                            columns["DEL_KVARH_LAG"] if "DEL_KVARH_LAG" in columns else None
                        )
                        del_kvarh_lead = (
                            columns["DEL_KVARH_LEAD"] if "DEL_KVARH_LEAD" in columns else None
                        )
                        rec_kvarh_lag = (
                            columns["REC_KVARH_LAG"] if "REC_KVARH_LAG" in columns else None
                        )
                        rec_kvarh_lead = (
                            columns["REC_KVARH_LEAD"] if "REC_KVARH_LEAD" in columns else None
                        )
                if source != "HES-TMAP20210525":
                    lp_raw_temp = {
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
                        "start_strm_time": start_strm_time,
                        "warn_dur_ts": log_start_time,
                        "main_dur_ts": "2023-05-25 11:13:00",
                        "rt_count": rt_count,
                    }
                    print(lp_raw_temp)
                    lp_raw_temp = func.check_data(
                        lp_raw_temp, flowfile_attr, file_seqno, read_group, rt_count, wait_cnt
                    )
                    print(lp_raw_temp)
                else:
                    lpi_raw_temp = {
                        "source": source,
                        "meter_id": meter_id,
                        "fan_id": "",
                        "tamp_cust_id": meter_id,  # not sure
                        "comment": "",
                        "read_time": read_time,
                        "interval": interval,
                        "note": note,
                        "del_kwh": del_kwh,
                        "rec_kwh": rec_kwh,
                        "del_kvarh_lag": del_kvarh_lag,
                        "del_kvarh_lead": del_kvarh_lead,
                        "rec_kvarh_lag": rec_kvarh_lag,
                        "rec_kvarh_lead": rec_kvarh_lead,
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
                        "start_strm_time": "",
                        "warn_dur_ts": "",
                        "main_dur_ts": "",
                        "rt_count": rt_count,
                    }
                    lpi_raw_temp = func.check_data(
                        lpi_raw_temp, flowfile_attr, file_seqno, read_group, rt_count, wait_cnt
                    )
                    print(lpi_raw_temp)

                # ---------------------------------------------------------------------------- #
                #                             lp_raw with main_data                            #
                # ---------------------------------------------------------------------------- #
                if MAIN_ENABLE_MK == 1:
                    main_start_time = datetime.now()
                    if read_time is not None:
                        read_time_ux = int(
                            datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp()
                        )

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
                        if source != "HES-TMAP20210525":
                            func.combine_maindata(main_srch, lp_raw_temp, main_start_time)
                            main_succ_cnt += 1
                            func.publish_kafka(
                                lp_raw_temp,
                                "mdes.stream.lp-raw",
                                func.hash_func(meter_id),
                            )
                        else:
                            func.combine_maindata(main_srch, lpi_raw_temp, main_start_time)
                            main_succ_cnt += 1
                            func.publish_kafka(
                                lpi_raw_temp,
                                "mdes.stream.lpi-raw",
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
                        main_dur_ts = str(datetime.now() - main_start_time)
                        read_time_int = int(
                            datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp()
                        )

                        if source != "HES-TMAP20210525":
                            func.set_nomaindata(
                                lp_raw_temp,
                                main_dur_ts,
                                read_time_int,
                                meter_id,
                                file_dir_date,
                                redis_conn,
                            )
                        else:
                            func.set_nomaindata(
                                lpi_raw_temp,
                                main_dur_ts,
                                read_time_int,
                                meter_id,
                                file_dir_date,
                                redis_conn,
                            )
                else:
                    if source != "HES-TMAP20210525":
                        func.publish_kafka(
                            lp_raw_temp,
                            "mdes.stream.lp-raw",
                            func.hash_func(meter_id),
                        )
                    else:
                        func.publish_kafka(
                            lpi_raw_temp,
                            "mdes.stream.lpi-raw",
                            func.hash_func(meter_id),
                        )
            meters[meter_id] += 1
            total_cnt = meters[meter_id]
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
            # print("Duration: {}".format(datetime.now() - start_time))
            sys.exit(exitcode)

        # ---------------------------------------------------------------------------- #
        #                             lp_raw with main_data                            #
        # ---------------------------------------------------------------------------- #

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
