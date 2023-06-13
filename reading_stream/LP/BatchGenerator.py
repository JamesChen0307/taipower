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
                "select count(*) as jobcnt from ami_dg.bucket_ctrl_log where proc_type = 1;"
            )

            jobcnt = result_jobcnt[0][0]

            if jobcnt > 1:
                exitcode = 1
                sys.exit(exitcode)
            else:
                result_filebatchno = func.gp_search(
                    """
                    select file_batch_no
                    from ami_dg.bucket_ctrl_log
                    where proc_type = 0 and enable_mk = 1
                    order by crtd_time asc
                    limit 1
                    """
                )
                file_batch_no = result_filebatchno[0][0]

                result_pathobj = func.gp_search(
                    """
                        select
                        file_batch_no,
                        bucket_nm,
                        read_group,
                        to_char(date_trunc('month', dt), 'yyyy-mm-dd') as file_dir_ym,
                        to_char(dt, 'yyyy-mm-dd') as file_dir_date,
                        case
                            when bucket_nm is not null then bucket_nm || '/' || lower(read_group) || to_char(dt, '/yyyy/mm/dd/')
                            else path_nm
                        end as file_path,
                        0 as file_cnt,
                        0 as proc_cnt,
                        batch_mk,
                        0 as proc_type,
                        TO_CHAR(now(), 'YYYY-MM-DD HH24:MI:SS') as crtd_time,
                        null as log_start_time,
                        null as log_upd_time,
                        null as log_end_time
                        from
                        (
                            select
                            file_batch_no,
                            bucket_nm,
                            path_nm,
                            read_group,
                            start_date,
                            end_date,
                            batch_mk,
                            generate_series(start_date, end_date, interval '1 day') as dt
                            from
                            (
                                select
                                file_batch_no,
                                bucket_nm,
                                path_nm,
                                read_group,
                                start_date,
                                end_date,
                                batch_mk
                                from
                                ami_dg.bucket_ctrl_log
                                where
                                proc_type = 0
                                and enable_mk = 1
                                order by
                                crtd_time asc
                                limit
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
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
