"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 將主資料庫讀表值(greenplum)載入至Redis暫時資料庫
  (2) 更新待處理載入清單狀態
"""
# Author：JamesChen
# Date：2023/O6/09
#
# Modified by：[V0.01][20230531][JamesChen][]

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
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

    try:
        # ---------------------------------------------------------------------------- #
        #                               Flowfile Content                               #
        # ---------------------------------------------------------------------------- #

        # 讀取Flowfile內容
        flowfile_json = sys.stdin.buffer.read().decode("utf-8")
        flowfile_data = json.loads(flowfile_json)

        src_flow = flowfile_data["src_flow"]
        source = flowfile_data["source"]
        meter_id = flowfile_data["meter_id"]
        read_group = flowfile_data["read_group"]
        read_time = flowfile_data["read_time"]
        read_time_int = flowfile_data["read_time_int"]
        read_date = datetime.strptime(read_time, DATE_FORMAT).strftime("%Y-%m-%d")
        last_date = datetime.strptime(read_date, DATE_FORMAT) - timedelta(days=1)
        last_date_int = int(last_date.timestamp())
        dupstat_key = "dup_stat:" + meter_id + "_" + read_time
        lpdata_key = "lp_data:" + meter_id + "_" + datetime.strptime(read_time, DATE_FORMAT)
        lpidata_key = "lpi_data:" + meter_id + "_" + datetime.strptime(read_time, DATE_FORMAT)

        # 檢查有無dup_stat:{key}資料
        dupstat_result = redis_conn.execute_command(
            "JSON.GET",
            dupstat_key,
            "$.data[?(@.read_time_int=={read_time_int})]".format(read_time_int=read_time_int),
        )

        if src_flow == "LP_Validate" and dupstat_result is None:
            # 自主資料庫data_dup_stat載入關於本串流值之過往重複紀錄
            dup_search = func.gp_search(
                f"""
                SELECT
                    source,
                    read_group,
                    meter_type,
                    meter_id,
                    read_time,
                    read_time_int,
                    read_time_bias,
                    dup_cnt,
                    log_start_time,
                    log_upd_time,
                    log_end_time
                FROM
                    ami_dg.data_dup_stat
                WHERE
                    source = '{source}'
                    AND meter_id = '{meter_id}'
                    AND read_group = '{read_group}'
                    AND read_time = '{read_time}'
            """.format(
                    source=source, meter_id=meter_id, read_group=read_group, read_time=read_time
                )
            )
            dup_search["log_date_int"] = int(datetime.now().timestamp())
            if redis_conn.exists(dupstat_key):
                dup_data = {
                    "read_date_int": int(datetime.strptime(read_date, "%Y-%m-%d").timestamp()),
                    "data": dup_search,
                }
                func.set_redis(redis_conn, dupstat_key, dup_search)
            else:
                redis_conn.execute_command("JSON.ARRAPPEND", dupstat_key, "$.data", dup_search)
        elif src_flow == "LP_TmapProcess" or "LP_Imputation":
            if read_group == "LP":
                last_result = redis_conn.execute_command(
                    "JSON.GET",
                    lpdata_key,
                    "$.data[?(@.read_time_int=={read_time_int})]".format(
                        read_time_int=last_date_int
                    ),
                )
            else:
                last_result = redis_conn.execute_command(
                    "JSON.GET",
                    lpidata_key,
                    "$.data[?(@.read_time_int=={read_time_int})]".format(
                        read_time_int=last_date_int
                    ),
                )
            if last_result is None:
                lpmiss_key = "lp_miss_data:" + meter_id
                if redis_conn.exists(lpmiss_key):
                    miss_result = redis_conn.execute_command(
                        "JSON.GET",
                        lpmiss_key,
                        "$.data[?(@.miss_read_date_int=={last_date_int})]".format(
                            last_date_int=last_date_int
                        ),
                    )
                    refer_min_read_time = miss_result["refer_min_read_time"]
                    refer_max_read_time = miss_result["refer_max_read_time"]
                    refer_cnt = miss_result["refer_cnt"]

                    if datetime.strptime(refer_min_read_time, DATE_FORMAT) > datetime.strptime(
                        read_time, DATE_FORMAT
                    ):
                        refer_min_read_time = read_time
                    if datetime.strptime(refer_max_read_time, DATE_FORMAT) < datetime.strptime(
                        read_time, DATE_FORMAT
                    ):
                        refer_max_read_time = read_time
                    if miss_result:
                        refer_cnt += 1

                    miss_record = {
                        "miss_read_date": last_date,
                        "miss_read_date_int": last_date_int,
                        "refer_min_read_time": refer_min_read_time,
                        "refer_max_read_time": refer_max_read_time,
                        "refer_min_read_time_int": int(
                            datetime.strptime(refer_min_read_time, DATE_FORMAT).timestamp()
                        ),
                        "refer_max_read_time_int": int(
                            datetime.strptime(refer_max_read_time, DATE_FORMAT).timestamp()
                        ),
                        "refer_cnt": refer_cnt,
                        "log_upd_time": datetime.now().strftime(DATE_FORMAT),
                    }
                    if miss_result:
                        redis_conn.execute_command("JSON.SET", "lpmiss_key", ".data", miss_record)
                    else:
                        miss_record["log_start_time"] = datetime.now().strftime(DATE_FORMAT)
                        redis_conn.execute_command(
                            "JSON.ARRAPPEND", lpmiss_key, "$.data", miss_record
                        )
                else:
                    miss_record = {
                        "source": source,
                        "meter_id": meter_id,
                        "data": {
                            "miss_read_date": last_date,
                            "miss_read_date_int": last_date_int,
                            "refer_min_read_time": read_time,
                            "refer_max_read_time": read_time,
                            "refer_min_read_time_int": read_time_int,
                            "refer_max_read_time_int": read_time_int,
                            "refer_cnt": 1,
                            "log_start_time": datetime.now().strftime(DATE_FORMAT),
                            "log_upd_time": "",
                            "log_end_time": "",
                        },
                    }
                    redis_conn.execute_command("JSON.SET", lpmiss_key, ".", miss_record)

        # 依據串流值src_flow、read_group判斷轉拋的Kafka Topic
        if src_flow == "LP_Validate" and read_group == "LP":
            topic = "mdes.stream.lp-raw-redo"
        elif src_flow == "LP_Validate" and read_group == "LPI":
            topic = "mdes.stream.lpi-raw-redo"
        elif src_flow == "LP_TmapProcess":
            topic = "mdes.stream.lpi-preprocessed"
        elif src_flow == "LP_Imputation":
            topic = "mdes.stream.lp-preprocessed"

        func.publish_kafka(flowfile_data, topic, func.hash_func(meter_id))

    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_Dataload",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
