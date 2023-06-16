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
        miss_keys = redis_conn.keys("lp_miss_stat*")

        # 以JSON.GET方式取得，採多執行緒
        mp_results = [mppool.apply_async(func.get_redis, (key,)) for key in miss_keys]
        mppool.close()
        mppool.join()

        mp_data = [res.get() for res in mp_results]
        decoded_data = [json.loads(d.decode("utf-8")) for d in mp_data]

        for data in decoded_data:
            source = data["source"]
            read_group = data["read_group"]
            meter_type = data["meter_type"]
            meter_id = data["meter_id"]
            miss_read_date = data["miss_read_date"]

            missstat_search = func.gp_search(
                """
                    SELECT
                        ref_cnt
                    FROM
                        ami_dg.lp_dup_meter_log
                    WHERE
                        source = {0}
                        AND read_group {1}
                        AND meter_type = {2}
                        AND meter_id = {3}
                        AND miss_read_date = {4}
                """.format(
                    source, read_group, meter_type, meter_id, miss_read_date
                )
            )
            #  ami_dg.data_miss_stat沒有對應的暫存物件資訊，則以新增方式將資料新增至ami_dg.data_miss_stat
            if len(missstat_search) > 1:
                ref_cnt_gp = missstat_search[0][0]
                ref_cnt = data["ref_cnt"] + ref_cnt_gp
                update_values = {
                    "refer_min_read_time": data["refer_min_read_time"],
                    "refer_max_read_time": data["refer_max_read_time"],
                    "ref_cnt": ref_cnt,
                    "log_end_time": datetime.now().strftime(DATE_FORMAT),
                }
                condition_values = {
                    "source": source,
                    "read_group": read_group,
                    "meter_type": meter_type,
                    "meter_id": meter_id,
                    "miss_read_date": miss_read_date
                }
                func.gp_update_v2("ami_dg.data_miss_stat", update_values, condition_values)
            #  ami_dg.data_miss_stat沒有對應的暫存物件資訊，則以新增方式將資料新增至ami_dg.data_miss_stat
            else:
                insert_dict = {
                    "source": source,
                    "read_group": read_group,
                    "meter_type": meter_type,
                    "meter_id": meter_id,
                    "miss_read_date": miss_read_date,
                    "refer_min_read_time": data["refer_min_read_time"],
                    "refer_max_read_time": data["refer_max_read_time"],
                    "refer_cnt": data["refer_cnt"],
                    "log_start_time": data["log_start_time"],
                    "log_upd_time": data["log_upd_time"],
                    "log_end_time": datetime.now().strftime(DATE_FORMAT),
                }
                func.gp_insert("ami_dg.data_miss_stat", insert_dict)
        # 更新至主資料庫完成之後需刪除對應的Redis物件
        redis_conn.delete(*missstat_search)

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
