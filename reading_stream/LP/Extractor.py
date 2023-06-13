"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 取得壓縮前的檔名
"""
# Author：JamesChen
# Date：2023/O5/17
#
# Modified by：[V0.01][20230531][JamesChen][改從S3直接取得解壓縮前的檔名]

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
        bucket = sys.argv[1]
        raw_gzfile = sys.argv[2]
        gz_name = func.get_gzinfo(bucket, raw_gzfile)
        print(gz_name)
    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_GetS3File",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
