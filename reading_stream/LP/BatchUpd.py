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

        # 查詢Batch作業清單
        batch_result = func.gp_search(
            """
                SELECT
                    DISTINCT file_batch_no
                FROM
                    ami_dg.file_batch_log
                WHERE
                    proc_type <> 6;
            """
        )
        file_batch_no = batch_result[0]
        if file_batch_no != 0:
            filelog_result = redis_conn.execute_command(
                "FT.SEARCH",
                "filelog_idx",
                "@file_batch_no:{0}".format(file_batch_no),
                "RETURN",
                "3",
                "$.raw_file",
                "$.file_dir_date",
                "$.proc_type",
            )

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
