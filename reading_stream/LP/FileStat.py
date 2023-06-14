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

    try:
        # ---------------------------------------------------------------------------- #
        #                               Flowfile Content                               #
        # ---------------------------------------------------------------------------- #


        cpus = mp.cpu_count()
        mppool = mp.Pool(processes=cpus)

        # 先取得查詢筆數
        limit = redis_conn.execute_command(
            "FT.AGGREGATE",
            "filelog_idx",
            "@log_date_int:[-inf {0}] -@proc_type:[6 6]".format(int(datetime.now().timestamp())),
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
            "filelog_idx",
            "@log_date_int:[-inf 2686200000] -@proc_type:[6 6]",
            "NOCONTENT",
            "LIMIT",
            "0",
            limit_num,
        )

        search_result = search_result[1:]

        # 以JSON.GET方式取得，採多執行緒
        mp_results = [mppool.apply_async(func.redis_get, (key,)) for key in search_result]
        mppool.close()
        mppool.join()

        mp_data = [res.get() for res in mp_results]
        decoded_data = [json.loads(d.decode("utf-8")) for d in mp_data]

        # 將各檔案資訊載入至pandas dataframe統整處理
        df = pd.DataFrame(decoded_data)

        df["proc_days"] = (
            datetime.now().timestamp()
            - pd.to_datetime(df["log_start_time"]).astype("int64") / 10**9
        ) // (24 * 60 * 60)
        df["proc_days"] = df["proc_days"].astype(int)

        df = df[
            [
                "source",
                "read_group",
                "file_dir_ym",
                "file_dir_date",
                "proc_days",
                "proc_type",
                "batch_mk",
                "raw_file",
                "total_cnt",
                "warn_cnt",
                "main_succ_cnt",
                "dedup_cnt",
                "err_cnt",
                "dup_cnt",
                "hist_cnt",
                "wait_cnt",
                "fnsh_cnt",
            ]
        ]

        # 依據 source、read_group、file_dir_ym、file_dir_date、proc_days、proc_type、batch_mk彙加各項筆數統計數
        stats_result = (
            df.groupby(
                [
                    "source",
                    "read_group",
                    "file_dir_ym",
                    "file_dir_date",
                    "proc_days",
                    "proc_type",
                    "batch_mk",
                ]
            )
            .agg(
                {
                    "total_cnt": "sum",
                    "warn_cnt": "sum",
                    "main_succ_cnt": "sum",
                    "dedup_cnt": "sum",
                    "err_cnt": "sum",
                    "dup_cnt": "sum",
                    "hist_cnt": "sum",
                    "wait_cnt": "sum",
                    "fnsh_cnt": "sum",
                    "raw_file": "count",
                }
            )
            .rename(columns={"raw_file": "file_cnt"})
            .reset_index()
        )

        # truncate既有ami_dg.data_file_stat
        func.gp_truncate("ami_dg.data_file_stat")
        gp_stats = stats_result.to_dict(orient="records")
        gp_stats["log_date_time"] = datetime.now().strftime(DATE_FORMAT)

        # 將統計資料新增至ami_dg.data_file_stat
        func.gp_insert("ami_dg.data_file_stat", gp_stats)

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
