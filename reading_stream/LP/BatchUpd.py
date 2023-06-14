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


    def _proc_days_count(file_batch_no):
        # 依據前述步驟取得的{file_batch_no}清單
        # 檢查各{file_batch_no}下已完成處理的日期目錄共幾日，以及與bucket_ctrl_log的待處理天數是否相符
        days_result = func.gp_search(
            """
                SELECT
                    a.proc_days,
                    b.end_date - b.start_date + 1 AS num_days
                FROM
                (
                    SELECT
                    `   file_batch_no,
                    `   COUNT(*) AS proc_days
                    FROM
                        ami_dg.path_batch_log
                    WHERE
                        file_batch_no = '{file_batch_no}'
                        AND proc_type = 2
                    GROUP BY
                        file_batch_no
                ) a
                JOIN ami_dg.bucket_ctrl_log b ON a.file_batch_no = b.file_batch_no
                WHERE
                    b.file_batch_no = '{file_batch_no}';
            """.format(file_batch_no=file_batch_no)
        )
        proc_days = days_result[0][0]
        num_days = days_result[0][1]
        update_values = {}
        condition_values = {}
        if proc_days == num_days:
            update_values = {
                "proc_type": proc_type,
                "log_end_time": int(datetime.now().timestamp())
            }
            condition_values = {
                "file_batch_no": file_batch_no
            }
            func.gp_update_v2("ami_dg.bucket_ctrl_log", update_values,condition_values)
        else:
            update_values = {
                "log_upd_time": int(datetime.now().timestamp())
            }
            condition_values = {
                "file_batch_no": file_batch_no
            }
            func.gp_update_v2("ami_dg.bucket_ctrl_log", update_values,condition_values)
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

        for no in batch_result:
            file_batch_no = no[0]
            # 依據上述回傳作業清單之每筆{file_batch_no}查詢Redis filelog取得各個檔案{proc_type}處理狀態
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
            parsed_result = []
            for i in range(0, len(filelog_result), 2):
                values = [
                    item.decode("utf-8").lstrip("$.") if isinstance(item, bytes) else item
                    for item in filelog_result[i + 1]
                ]
                parsed_result.append(dict(zip(values[0::2], values[1::2])))

            # 輸出解碼後的結果
            for item in parsed_result:
                proc_type = item["proc_type"]
                item.pop("proc_type", None)
                item["file_batch_no"] = file_batch_no
                # 根據file_batch_no、raw_file、file_dir_date將狀態更新回ami_dg.file_batch_log
                if proc_type == 6:
                    update_values = {
                        "proc_type": proc_type,
                        "log_end_time": int(datetime.now().timestamp())
                    }
                    func.gp_update_v2("ami_dg.file_batch_log", update_values, item)
                else:
                    update_values = {
                        "proc_type": proc_type,
                        "log_upd_time": int(datetime.now().timestamp())
                    }
                    func.gp_update_v2("ami_dg.file_batch_log", update_values, item)

            # 統計各個{file_batch_no}統計狀態
            stats_batchno = func.gp_search(
                """
                    SELECT
                        file_batch_no,
                        file_dir_date,
                        batch_mk,
                        proc_type,
                        SUM(proc_cnt) OVER (PARTITION BY file_batch_no, file_dir_date) AS file_cnt,
                        proc_cnt
                    FROM
                    (
                        SELECT
                            file_batch_no,
                            file_dir_date,
                            batch_mk,
                            proc_type,
                            COUNT(*) AS proc_cnt
                        FROM
                            ami_dg.file_batch_log
                        WHERE
                            file_batch_no = '{0}'
                        GROUP BY
                            file_batch_no,
                            file_dir_date,
                            batch_mk,
                            proc_type
                    ) a;
                """.format(file_batch_no)
            )

            # 代表該目錄已完成處理，故更新ami_dg.path_batch_log該筆file_batch_no、file_dir_date統計及處理狀態
            if len(stats_batchno) == 1:
                file_batch_no = stats_batchno[0][0]
                file_dir_date = stats_batchno[0][1]
                proc_type = stats_batchno[0][3]
                file_cnt = stats_batchno[0][4]
                proc_cnt = stats_batchno[0][5]
                if proc_type == 6 and file_cnt == proc_cnt:
                    update_values = {
                        "file_cnt": file_cnt,
                        "proc_cnt": proc_cnt,
                        "proc_type": 2,
                        "log_end_time": int(datetime.now().timestamp())
                    }
                    condition_values = {
                        "file_batch_no":file_batch_no,
                        "file_dir_date": file_dir_date
                    }
                    func.gp_update_v2("ami_dg.path_batch_log", update_values, condition_values)

                # 依據前述步驟取得的{file_batch_no}清單
                # 檢查各{file_batch_no}下已完成處理的日期目錄共幾日，以及與bucket_ctrl_log的待處理天數是否相符
                _proc_days_count(file_batch_no)

            # 表示檔案未處理完成，故更新ami_dg.path_batch_log該筆file_batch_no、file_dir_date統計及處理狀態
            elif len(stats_batchno) != 1:
                total_proc = 0
                for result in stats_batchno:
                    proc_type = result[3]
                    file_cnt = result[4]
                    proc_cnt = result[5]
                    # 除proc_type=6之外的1~N筆不同proc_type的#proc_cnt#加總
                    if file_cnt != proc_cnt and proc_type != 6:
                        total_proc += proc_cnt
                file_batch_no = stats_batchno[0][0]
                file_dir_date = stats_batchno[0][1]
                file_cnt = stats_batchno[0][4]
                update_values = {
                    "file_cnt": file_cnt,
                    "proc_cnt": total_proc
                }
                condition_values = {
                    "file_batch_no": file_batch_no,
                    "file_dir_date": file_dir_date
                }
                func.gp_update_v2("ami_dg.path_batch_log", update_values, condition_values)

                # 依據前述步驟取得的{file_batch_no}清單
                # 檢查各{file_batch_no}下已完成處理的日期目錄共幾日，以及與bucket_ctrl_log的待處理天數是否相符
                _proc_days_count(file_batch_no)


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
