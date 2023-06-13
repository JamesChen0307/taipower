"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) Load Profile 產製批次檔案訂單作業(file_batch_log)
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
from datetime import datetime
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
        flowfile_json = sys.stdin.buffer.read().decode("utf-8")
        flowfile_data = json.loads(flowfile_json)
        todo = int(flowfile_data[0]["todo"])

        if todo < 1:
            exitcode = 1
            sys.exit(exitcode)
        else:
            result_jobcnt = func.gp_search(
                "select count(*) as jobcnt from ami_dg.path_batch_log where proc_type = 1;"
            )

            jobcnt = result_jobcnt[0][0]

            if jobcnt > 1:
                exitcode = 1
                sys.exit(exitcode)
            else:
                result_pathbatch = func.gp_search(
                    """
                    select
                    file_batch_no,
                    bucket_nm,
                    file_path,
                    file_dir_ym,
                    file_dir_date,
                    read_group,
                    batch_mk
                    from
                    ami_dg.path_batch_log
                    where
                    proc_type = 0
                    order by
                    crtd_time asc
                    limit
                    1
                    """
                )
                pathbatch_dict = dict(
                    zip(
                        [
                            "file_batch_no",
                            "bucket_nm",
                            "file_path",
                            "file_dir_ym",
                            "file_dir_date",
                            "file_path",
                            "read_group",
                            "batch_mk",
                        ],
                        result_pathbatch[0],
                    )
                )

                func.gp_update(
                    "UPDATE ami_dg.path_batch_log SET proc_type = %s WHERE file_batch_no = %s;",
                    1,
                    pathbatch_dict["file_batch_no"],
                )
                prefix = pathbatch_dict["file_path"].split("/", 1)[1]
                s3_list = func.list_s3object(
                    pathbatch_dict["bucket_nm"], prefix, pathbatch_dict["read_group"]
                )
                for obj in s3_list:
                    file_dict = {
                        "file_batch_no": pathbatch_dict["file_batch_no"],
                        "bucket_nm": pathbatch_dict["bucket_nm"],
                        "read_group": pathbatch_dict["read_group"],
                        "file_dir_ym": pathbatch_dict["file_dir_ym"],
                        "file_dir_date": pathbatch_dict["file_dir_date"],
                        "batch_mk": pathbatch_dict["batch_mk"],
                        "file_type": obj.split(".")[-1],
                        "file_path": obj.rsplit("/", 1)[0],
                        "file_name": obj.rsplit("/", 1)[-1],
                        "filename": obj,
                        "log_start_time": datetime.now().strftime(DATE_FORMAT),
                        "ctrd_time": datetime.now().strftime(DATE_FORMAT),
                        "proc_type": 1,
                    }

                    func.publish_kafka(file_dict, "mdes.stream.file-batch-log")
                    try:
                        func.gp_insert("ami_dg.file_batch_log", file_dict)
                    except Exception as e:
                        func.gp_update(
                            "UPDATE ami_dg.bucket_ctrl_log SET proc_type = %s WHERE file_batch_no = %s;",
                            4,
                            pathbatch_dict["file_batch_no"],
                        )
                        exitcode = 1
                        sys.exit(exitcode)
                file_cnt = len(s3_list)
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
