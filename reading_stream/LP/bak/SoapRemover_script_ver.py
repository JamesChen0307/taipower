"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 移除 XML Soap 封裝
  (2)
"""

# Author：JamesChen
# Date：2023/O5/15
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import re

from java.nio.charset import StandardCharsets
from org.apache.commons.io import IOUtils
from org.apache.nifi.processor.io import StreamCallback


class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        cdata_regex = r"<!\[CDATA\[(.*?)\]\]>"
        cdata_content = ""
        # 讀取flowfile的內容
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        # 將SOAP的內容排除
        matches = re.findall(cdata_regex, text, re.DOTALL)
        if matches:
            cdata_content = matches[0]
        else:
            cdata_content = text
        # 將去除SOAP的LP傳到flowfile中
        outputStream.write(bytearray(cdata_content.encode("utf-8")))


flowFile = session.get()
if flowFile != None:
    flowFile = session.write(flowFile, PyStreamCallback())
    session.transfer(flowFile, REL_SUCCESS)
