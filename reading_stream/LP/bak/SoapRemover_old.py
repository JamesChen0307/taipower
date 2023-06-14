#!/usr/bin/ python3
"""
 Code Desctiption：
 
 LP 讀表串流處理作業
  (1) 移除 XML Soap 封裝
  (2) 
"""
# Author：OOO
# Date：OOOO/OO/OO
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import sys

sys.path.append("/usr/lib/python3/dist-packages")
import gzip
import json
import re
from datetime import datetime

import constant
import gzinfo
import pandas as pd
import xmltodict
from ami import lp_config
from confluent_kafka import Producer

if __name__ == "__main__":
    exitcode = 0

    # 搜尋CDATA內容
    cdata_regex = r"<!\[CDATA\[(.*?)\]\]>"

    # f = sys.stdin
    file = sys.argv[1]
    try:
        # delimeter = "\\"
        delimeter = "/"
        part = file.split(delimeter)
        path = delimeter.join(part[0:-1])
        file_name = part[-1]
        file_type = file_name.split(".")[1].lower()

        vTime = file_name.split(".")[0].split("_")[2]
        rec_time = (
            vTime[0:4]
            + "-"
            + vTime[4:6]
            + "-"
            + vTime[6:8]
            + "T"
            + vTime[8:10]
            + ":"
            + vTime[10:12]
            + ":"
            + vTime[12:14]
        )

        cdata_content = ""
        # 讀取SOAP.xml檔案
        with open(file, "r", encoding="utf-8") as fd:
            data = fd.read()
            matches = re.findall(cdata_regex, data, re.DOTALL)
            if matches:
                # 僅取soap封裝內容
                cdata_content = matches[0]
                print(cdata_content)

    except Exception as e:
        print("2", e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
