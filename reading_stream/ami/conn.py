#!/usr/bin/ python3
"""
 Code Desctiption：

 讀表串流處理作業-共用 constant 連線資訊
"""
# Author：babylon@cht.com.tw
# Date：2023/04/29
#
# Modified by：
#

"""
 Connection
"""
#KAFKA
MDES_KAFKA_URL = "172.31.48.34:9092"
MDES_KAFKA_PARTITIONS = 3

#REDIS
MDES_REDIS_HOST = "172.31.48.67"
MDES_REDIS_PORT = 12000
MDES_REDIS_DB = 0
MDES_REDIS_USER = "default"
MDES_REDIS_PASS = "asdf1234"
MDES_REDIS_TTL = 5184000 #Temporary 60 days

#GREENPLUM
MDES_GP_HOST = "172.31.48.41"
MDES_GP_PORT = 5432
MDES_GP_DB = "taipower"
MDES_GP_USER = "mdesadm"
MDES_GP_PASS = "password"

#S3 
MDES_S3_ACCESS_KEY = "AKIAF4944F72BEBA94F4"
MDES_S3_SECRET_KEY = "JoktT3GDO+WXg+hrGUx7zJWDOiVDn6OsvPdZ4esa"
MDES_S3_URL = "http://172.31.48.247:9020"
