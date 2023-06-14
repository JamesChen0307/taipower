"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 協助每日定期或定時將log和統計資料更新至主資料庫，直至作業完成
"""
# Author：JamesChen
# Date：2023/O6/13
#
# Modified by：[V0.01][20230531][JamesChen][]

import json
import logging
import multiprocessing as mp
import os
import sys
from datetime import datetime
from os.path import dirname

import pandas as pd
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

    # ---------------------------------------------------------------------------- #
    #                               Multi Processing                               #
    # ---------------------------------------------------------------------------- #
    cpus = mp.cpu_count()
    mppool = mp.Pool(processes=cpus)

    try:
        # ---------------------------------------------------------------------------- #
        #                               Flowfile Content                               #
        # ---------------------------------------------------------------------------- #

        system_time_int = int(datetime.now().timestamp())
        # 先取得查詢筆數
        limit = redis_conn.execute_command(
            "FT.AGGREGATE",
            "lp_dup_meter_idx",
            "@log_date_int:[-inf {0}] -@proc_type:[6 6]".format(system_time_int),
            "GROUPBY",
            "0",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "limit_num",
        )
        limit_num = limit[1][1]

        # 依據ttl_cnt取得清單資訊
        search_result = redis_conn.execute_command(
            "FT.SEARCH",
            "lp_dup_meter_idx",
            "@log_date_int:[-inf {0}] -@proc_type:[6 6]".format(system_time_int),
            "NOCONTENT",
            "LIMIT",
            "0",
            limit_num,
        )

        search_result = search_result[1:]

        # 以JSON.GET方式取得，採多執行緒
        mp_results = [mppool.apply_async(func.get_redis, (key,)) for key in search_result]
        mppool.close()
        mppool.join()

        mp_data = [res.get() for res in mp_results]
        decoded_data = [json.loads(d.decode("utf-8")) for d in mp_data]

        for data in decoded_data:
            source = data["source"]
            meter_type = data["meter_type"]
            meter_id = data["meter_id"]
            read_time = data["read_time"]
            read_time_bias = data["read_time_bias"]
            rec_no = data["rec_no"]
            del_kwh = data["del_kwh"]
            interval = data["interval"]
            note = data["note"]

            log_upd_time = data["log_upd_time"]
            dupmeter_search = func.gp_search(
                """
                    SELECT
                        source,
                        meter_type,
                        meter_id,
                        read_time,
                        read_time_bias,
                        rec_no,
                        del_kwh,
                        interval,
                        note,
                        COUNT(*) as dup_meter_count
                    FROM
                        ami_dg.lp_dup_meter_log
                    WHERE
                        source = {0}
                        AND meter_type = {1}
                        AND meter_id = {2}
                        AND read_time = {3}
                        AND read_time_bias = {4}
                        AND rec_no = {5}
                        AND del_kwh = {6}
                        AND interval = {7}
                        AND note = {8}
                    GROUP BY
                        source,
                        meter_type,
                        meter_id,
                        read_time,
                        read_time_bias,
                        rec_no,
                        del_kwh,
                        interval,
                        note
                    HAVING COUNT(*) > 1;
                """.format(
                    meter_id, read_time
                )
            )
            #  ami_dg.data_dup_stat有對應的暫存物件統計資訊，則以update方式將資料更新至ami_dg.data_dup_stat
            if len(dupmeter_search) < 1:
                insert_dict = {
                    "source": source,
                    "meter_type": meter_type,
                    "meter_id": meter_id,
                    "read_time": read_time,
                    "read_time_bias": read_time_bias,
                    "data_no": data["data_no"],
                    "rec_no": rec_no,
                    "del_kwh": del_kwh,
                    "interval": interval,
                    "note": note,
                    "log_date_time": data["log_date_time"],
                }
                func.gp_insert("ami_dg.lp_dup_meter_log", insert_dict)
        # 更新至主資料庫完成之後需刪除對應的Redis物件
        redis_conn.delete(*search_result)

    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_Collector",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
