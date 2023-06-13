"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 移除 XML Soap 封裝
"""

# Author：JamesChen
# Date：2023/O5/17
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import re
import sys
import logging

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
        cdata_regex = r"<!\[CDATA\[(.*?)\]\]>"
        xml_content = ""
        data = sys.stdin.buffer.read().decode("utf-8")
        matches = re.findall(cdata_regex, data, re.DOTALL)
        if matches:
            # 僅取soap封裝內容
            xml_content = matches[0]
            print(xml_content)
        else:
            xml_content = data
            print(xml_content)
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
