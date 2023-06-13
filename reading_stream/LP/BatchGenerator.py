"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) Load Profile 產製批次目錄訂單作業(path_batch_log)
"""

# Author：JamesChen
# Date：2023/O6/02
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import json
import logging
import os
import sys
from os.path import dirname

CURRENT_DIR = dirname(__file__)
sys.path.append(os.path.abspath(CURRENT_DIR + "/../"))
from ami import func

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

    try:
        # ---------------------------------------------------------------------------- #
        #                               Flowfile Content                               #
        # ---------------------------------------------------------------------------- #

        # 讀取Flowfile內容
        flowfile_json = sys.stdin.buffer.read().decode('utf-8')
        flowfile_data = json.loads(flowfile_json)
        todo = flowfile_data[0]["todo"]

        if todo < 1:
            exitcode = 1
            sys.exit(exitcode)
        else:
            result_jobcnt = func.gp_search(
                """
                    SELECT
                        count(*) as jobcnt
                    FROM
                        ami_dg.bucket_ctrl_log
                    WHERE
                        proc_type = 1
                        AND read_group = 'LP';
                """
            )

            jobcnt = result_jobcnt[0][0]

            if jobcnt > 1:
                exitcode = 1
                sys.exit(exitcode)
            else:
                result_filebatchno = func.gp_search(
                    """
                        SELECT
                            file_batch_no
                        FROM
                            ami_dg.bucket_ctrl_log
                        WHERE
                            proc_type = 0
                            and enable_mk = 1
                            and read_group = 'LP'
                        ORDER BY
                            crtd_time asc
                        LIMIT
                            1;
                    """
                )
                file_batch_no = result_filebatchno[0][0]

                result_pathobj = func.gp_search(
                    """
                        SELECT
                            file_batch_no,
                            bucket_nm,
                            path_nm,
                            read_group,
                            TO_CHAR(DATE_TRUNC('month', dt), 'YYYY-MM-DD') AS file_dir_ym,
                            TO_CHAR(dt, 'YYYY-MM-DD') AS file_dir_date,
                            CASE
                                WHEN bucket_nm IS NOT NULL THEN bucket_nm || '/' || LOWER(read_group) || TO_CHAR(dt, '/YYYY/MM/DD/')
                                ELSE path_nm
                            END AS file_path,
                            batch_mk
                        FROM
                        (
                            SELECT
                                file_batch_no,
                                bucket_nm,
                                path_nm,
                                read_group,
                                start_date,
                                end_date,
                                batch_mk,
                                GENERATE_SERIES(start_date, end_date, INTERVAL '1 day') AS dt
                            FROM
                            (
                                SELECT
                                    file_batch_no,
                                    bucket_nm,
                                    path_nm,
                                    read_group,
                                    start_date,
                                    end_date,
                                    batch_mk
                                FROM
                                    ami_dg.bucket_ctrl_log
                                WHERE
                                    proc_type = 0
                                    AND enable_mk = 1
                                    AND read_group = 'LP'
                                ORDER BY
                                    crtd_time ASC
                                LIMIT
                                    1
                            ) a
                        ) b;
                    """
                )
                # 更新該筆訂單bucket_ctrl_log.proc_type=1表示處理中
                func.gp_update(
                    "UPDATE ami_dg.bucket_ctrl_log SET proc_type = %s WHERE file_batch_no = %s",
                    1,
                    file_batch_no,
                )

                for item in result_pathobj:
                    result_dict = dict(
                        zip(
                            [
                                "file_batch_no",
                                "bucket_nm",
                                "read_group",
                                "file_dir_ym",
                                "file_dir_date",
                                "file_path",
                                "file_cnt",
                                "proc_cnt",
                                "batch_mk",
                                "proc_type",
                                "crtd_time",
                                "log_start_time",
                                "log_upd_time",
                                "log_end_time",
                            ],
                            item,
                        )
                    )
                    try:
                        func.gp_insert("ami_dg.path_batch_log", result_dict)
                    except Exception as e:
                        func.gp_update(
                            "UPDATE ami_dg.bucket_ctrl_log SET proc_type = %s WHERE file_batch_no = %s",
                            4,
                            file_batch_no,
                        )
                        exitcode = 1
                        sys.exit(exitcode)

    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_Batch",
            exc_info=True,
        )
        '1'
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
