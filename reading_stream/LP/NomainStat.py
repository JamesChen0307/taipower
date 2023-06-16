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
        lpnomain_keys = redis_conn.keys("lp_raw_nomain*")
        lpinomain_keys = redis_conn.keys("lpi_raw_nomain*")

        # 以JSON.GET方式取得，採多執行緒
        mp_lp_results = [mppool.apply_async(func.get_redis_data, (key,)) for key in lpnomain_keys]
        mp_lpi_results = [mppool.apply_async(func.get_redis_data, (key,)) for key in lpinomain_keys]

        mppool.close()
        mppool.join()

        mp_lp_data = [res.get() for res in mp_lp_results]
        mp_lpi_data = [res.get() for res in mp_lpi_results]

        lp_decoded_data = [json.loads(d.decode("utf-8")) for d in mp_lp_data]
        lpi_decoded_data = [json.loads(d.decode("utf-8")) for d in mp_lpi_data]

        # 取得對應的資料欄位
        lp_list = []
        lpi_list = []

        for data in lp_decoded_data:
            source = data["source"]
            read_group = data["read_group"]
            meter_type = data["meter_type"]
            meter_id = data["meter_id"]
            read_time_bias = func.calculate_read_time_bias(data["read_time"])
            nomain_cnt = len(lp_decoded_data)
            log_date_time = datetime.now().strftime(DATE_FORMAT)

            lp_list.append({
                "source": source,
                "read_group": read_group,
                "meter_type": meter_type,
                "meter_id": meter_id,
                "read_time_bias": read_time_bias,
                "nomain_cnt": nomain_cnt,
                "log_date_time": log_date_time
            })

        for data in lpi_decoded_data:
            source = data["source"]
            read_group = data["read_group"]
            meter_type = data["meter_type"]
            meter_id = data["meter_id"]
            read_time_bias = func.calculate_read_time_bias(data["read_time"])
            nomain_cnt = len(lp_decoded_data)
            log_date_time = datetime.now().strftime(DATE_FORMAT)

            lpi_list.append({
                "source": source,
                "read_group": read_group,
                "meter_type": meter_type,
                "meter_id": meter_id,
                "read_time_bias": read_time_bias,
                "nomain_cnt": nomain_cnt,
                "log_date_time": log_date_time
            })


        # truncate既有ami_dg.data_nomain_stat後將暫存物件資訊以新增(insert)方式載入至ami_dg.data_nomain_stat
        func.gp_truncate("ami_dg.data_nomain_stat")
        for dict_data in lp_list:
            func.gp_insert("ami_dg.data_nomain_stat", dict_data)

        for dict_data in lpi_list:
            func.gp_insert("ami_dg.data_nomain_stat", dict_data)


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
