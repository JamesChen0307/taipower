#!/usr/bin/ python3
"""
 Code Desctiption：

 LP 讀表串流處理作業-共用 function
  (1) 格式修訂共用 function
  (2) Kafka Hash Partition function
  (3) .....
"""
# Author：OOO
# Date：2023/OO/OO
#
# Modified by：
#
import json
from datetime import datetime, timedelta

import boto3
import sys
import time
import redis
import psycopg2
from confluent_kafka import Producer
from pydantic import ValidationError
from typing import Optional
from kafka import KafkaProducer


from ami import conn, constant, lp_config


def fix_datetime(v):
    return ""


def fix_boolean(v):
    return ""


def fix_float(v):
    return ""


def fix_double(v):
    return ""


def fix_integer(v):
    if isinstance(v, (int, float)):
        return v
    elif isinstance(v, str) and v.isnumeric():
        return int(v)
    else:
        return ""


# Decimal 格式修正：取5整數、4小數
def fix_max_decimal(v):
    v1 = v.split(".")[0] if "." in v else v
    v2 = v.split(".")[1] if "." in v else ""
    s = v1[-5:] + ("." + v2[0:4]) if v2 else ""
    return s


# Decimal 格式修正：改為''
def fix_decimal(v):
    return ""


# Kafka Hash Partition
def hash_func(x):
    hash_val = hash(x)
    # Map the hash value to a partition number between 0 and no_of_parts-1
    partition_num = hash_val % conn.MDES_KAFKA_PARTITIONS
    return partition_num


def fix_W21010(v):
    int_part = int(v)
    int_len = len(str(int_part))
    int_fix = str(int_part)[-5:]
    dec_fix = str(v)[int_len + 1 :][:4]
    v_fix = float(int_fix + "." + dec_fix)
    return v_fix


def fix_max_double(v):
    int_part = int(v)
    int_len = len(str(int_part))
    int_fix = str(int_part)[-14:]
    dec_fix = str(v)[int_len + 1 :][:4]
    v_fix = float(int_fix + "." + dec_fix)
    return v_fix


def fix_W20006(v):
    return ""


def publish_kafka(input_data, topic, partition_id: Optional[int] = None):
    p = Producer({"bootstrap.servers": conn.MDES_KAFKA_URL})
    data = json.dumps(input_data)
    if partition_id is not None:
        p.produce(topic, value=data.encode("utf-8"), partition=partition_id)
    else:
        p.produce(topic, value=data.encode("utf-8"))
    p.flush()
    p.poll(0)



def publish_kafka_v2(input_data, topic, partition_id: Optional[int] = None):
    p = KafkaProducer(bootstrap_servers=conn.MDES_KAFKA_URL)
    data = json.dumps(input_data)
    message = data.encode("utf-8")
    p.send(topic, value=message, partition=partition_id)


def kafka_flush():
    p = Producer({"bootstrap.servers": conn.MDES_KAFKA_URL})
    p.flush()
    p.poll(0)


def decode_redis_nestedlist(lst):
    decoded_lst = []
    for item in lst:
        if isinstance(item, bytes):
            decoded_lst.append(item.decode())
        elif isinstance(item, list):
            decoded_lst.append(decode_redis_nestedlist(item))
        else:
            decoded_lst.append(item)
    return decoded_lst


def convert_redislist(lst):
    res_dct = {lst[i].decode("utf-8"): lst[i + 1].decode("utf-8") for i in range(0, len(lst), 2)}
    return res_dct


def set_redis(redis, key, data):
    redis.execute_command("JSON.SET", key, ".", json.dumps(dict(data)))
    redis.execute_command("EXPIRE", key, conn.MDES_REDIS_TTL)


def set_redis_data(redis, key, data):
    redis.execute_command("JSON.SET", key, ".data", json.dumps(dict(data)))


def publish_errorlog(file_dict, file_seqno, source, read_group, meter_id, read_time, type_cd):
    error_log = lp_config.ErrorLog(
        file_type=file_dict["file_type"],
        raw_gzfile=file_dict["raw_gzfile"],
        raw_file=file_dict["raw_file"],
        rec_time=file_dict["rec_time"],
        file_path=file_dict["file_path"],
        file_seqno=file_seqno,
        source=source,
        read_group=read_group,
        meter_id=meter_id,
        read_time=read_time,
        type_cd=type_cd,
        log_data_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    publish_kafka(dict(error_log), "mdes.stream.data-error-log", hash_func(meter_id))


def publish_warnlog(
    fileattr_dict, file_seqno, source, read_group, meter_id, read_time, type_cd, col_nm, rt_count
):
    warn_log = lp_config.WarnLog(
        file_type=fileattr_dict["file_type"],
        raw_gzfile=fileattr_dict["raw_gzfile"],
        raw_file=fileattr_dict["raw_file"],
        rec_time=fileattr_dict["rec_time"],
        file_path=fileattr_dict["file_path"],
        file_seqno=file_seqno,
        source=source,
        read_group=read_group,
        meter_id=meter_id,
        read_time=read_time,
        type_cd=type_cd,
        col_nm=col_nm,
        rt_count=rt_count,
        log_data_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    publish_kafka(dict(warn_log), "mdes.stream.data-warn-log", hash_func(meter_id))


def check_data(reading_data, fileattr_dict, file_seqno, read_group, rt_count, warn_cnt):
    """
    共用格式檢查 & 格式修正代碼
    """
    warn_cd = {
        "value_error.datetime": {"code": "W20001", "func": fix_datetime},
        "type_error.bool": {"code": "W20003", "func": fix_boolean},
        "type_error.float": {"code": "W20004", "func": fix_float},
        "type_error.decimal": {"code": "W20005", "func": fix_double},  # for double type
        "value_error.decimal.max_digits": {"code": "W20005", "func": fix_max_double},
        "type_error.integer": {"code": "W20006", "func": fix_integer},
        "W21010": {"func": fix_W21010},
        "W20006": {"func": fix_W20006},
    }

    try:
        warn_start_time = datetime.now()
        if reading_data["source"] == "HES-TMAP20210525":
            lp_config.LpiRawTemp(**reading_data)
        else:
            lp_config.LpRawTemp(**reading_data)
    except ValidationError as e:
        for v in e.errors():
            try:
                type_cd = warn_cd[v["type"]]["code"]
                column = v["loc"][0]
                old_val = reading_data[column]
                new_val = warn_cd[v["type"]]["func"](old_val)
                reading_data[column] = new_val

            except Exception:
                type_cd = v["msg"]
                column = v["loc"][0]
                old_val = reading_data[column]
                try:  # 格式修訂
                    new_val = warn_cd[v["msg"]]["func"](old_val)
                    reading_data[column] = new_val
                except Exception:  # 只報錯不修訂
                    publish_warnlog(
                        fileattr_dict,
                        file_seqno,
                        reading_data["source"],
                        read_group,
                        reading_data["meter_id"],
                        reading_data["read_time"],
                        type_cd,
                        column,
                        rt_count,
                    )
                    warn_cnt += 1
        # 格式錯誤 | 資料異常告警 Log
        publish_warnlog(
            fileattr_dict,
            file_seqno,
            reading_data["source"],
            read_group,
            reading_data["meter_id"],
            reading_data["read_time"],
            type_cd,
            column,
            rt_count,
        )
        warn_cnt += 1

    warn_dur_ts = (datetime.now() - warn_start_time).microseconds
    reading_data["warn_dur_ts"] = warn_dur_ts
    return reading_data


def combine_maindata(srch_result, reading_data, main_start_time):
    decode_result = decode_redis_nestedlist(srch_result)[2][3]  # $.data的資料
    main_data = json.loads(decode_result)
    reading_data.update(main_data)
    main_dur_ts = (datetime.now() - main_start_time).microseconds
    reading_data["main_dur_ts"] = main_dur_ts


def set_nomaindata(reading_data: dict, main_dur_ts, read_time_int, meter_id, file_dir_date, r):
    reading_data["main_dur_ts"] = main_dur_ts
    reading_data["read_time_int"] = read_time_int
    raw_json = json.dumps(reading_data)
    main_data_key = meter_id + "_" + file_dir_date
    main_data = {
        "meter_id": meter_id,
        "read_date": file_dir_date,
        "data": [],
    }
    maindata_json = json.dumps(main_data)
    if r.exists(main_data_key):
        r.execute_command("JSON.ARRAPPEND", main_data_key, ".data", raw_json)
    else:
        r.execute_command("JSON.SET", main_data_key, ".", maindata_json)
        r.execute_command("EXPIRE", main_data_key, conn.MDES_REDIS_TTL)
        r.execute_command("JSON.ARRAPPEND", main_data_key, ".data", raw_json)


def get_gzinfo(bucket, filename):
    client = boto3.client(
        "s3",
        endpoint_url=conn.MDES_S3_URL,
        aws_access_key_id=conn.MDES_S3_ACCESS_KEY,
        aws_secret_access_key=conn.MDES_S3_SECRET_KEY,
    )

    s3_response = client.get_object(Bucket=bucket, Key=filename)

    s3_object_body = s3_response.get("Body")
    original_filename = b""
    pos = 0
    while True:
        s = s3_object_body.read(1)
        val = bytes(b"\x00")
        if s == val:
            pos += 1
        if pos >= 1 and pos < 2:
            original_filename += s  # .rstrip('\x00')
        if pos > 2:
            break
        # print("orgin file: ", original_filename.decode("utf-8").strip('\x00'))
        return original_filename.decode("utf-8").strip("\x00")


def calculate_read_time_bias(read_time):
    read_time_obj = datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S")
    start_time_obj = read_time_obj.replace(hour=0, minute=0, second=0) + timedelta(minutes=15)
    print(start_time_obj - timedelta(minutes=15))
    end_time_obj = start_time_obj + timedelta(days=1) - timedelta(minutes=15)

    if read_time_obj == start_time_obj - timedelta(minutes=15):
        read_time_obj = read_time_obj - timedelta(days=1)
        return read_time_obj.strftime("%Y-%m-%d")
    elif start_time_obj <= read_time_obj < end_time_obj:
        return start_time_obj.strftime("%Y-%m-%d")
    else:
        return None


def gp_search(query_str):
    gp_conn = psycopg2.connect(
        host=conn.MDES_GP_HOST,
        port=conn.MDES_GP_PORT,
        database=conn.MDES_GP_DB,
        user=conn.MDES_GP_USER,
        password=conn.MDES_GP_PASS,
    )

    # 創建游標
    cur = gp_conn.cursor()

    # 執行 SQL 查詢
    cur.execute(query_str)

    # 獲取結果
    result = cur.fetchall()

    # 關閉游標和連接
    cur.close()
    gp_conn.close()

    # 回傳結果
    return result


def gp_update(query_str, update_value, condition_value):
    gp_conn = psycopg2.connect(
        host=conn.MDES_GP_HOST,
        port=conn.MDES_GP_PORT,
        database=conn.MDES_GP_DB,
        user=conn.MDES_GP_USER,
        password=conn.MDES_GP_PASS,
    )

    # 創建游標
    cur = gp_conn.cursor()

    # 執行 SQL 更新
    cur.execute(query_str, (update_value, condition_value))

    # 提交事務
    gp_conn.commit()

    # 關閉游標和連接
    cur.close()
    gp_conn.close()


def gp_update_v2(table_name, update_values, condition_values):
    gp_conn = psycopg2.connect(
        host=conn.MDES_GP_HOST,
        port=conn.MDES_GP_PORT,
        database=conn.MDES_GP_DB,
        user=conn.MDES_GP_USER,
        password=conn.MDES_GP_PASS,
    )

    # 創建游標
    cur = gp_conn.cursor()

    # 構建 SET 子句
    set_clause = ", ".join(f"{key} = %s" for key in update_values.keys())
    set_params = tuple(update_values.values())

    # 構建 WHERE 子句
    conditions = " AND ".join(f"{key} = %s" for key in condition_values.keys())
    condition_params = tuple(condition_values.values())

    # 構建完整的 SQL 更新語句
    full_query = f"UPDATE {table_name} SET {set_clause} WHERE {conditions}"

    # 執行 SQL 更新
    cur.execute(full_query, (*set_params, *condition_params))

    # 提交事務
    gp_conn.commit()

    # 關閉游標和連接
    cur.close()
    gp_conn.close()


def gp_insert(table_name, insert_dict):
    gp_conn = psycopg2.connect(
        host=conn.MDES_GP_HOST,
        port=conn.MDES_GP_PORT,
        database=conn.MDES_GP_DB,
        user=conn.MDES_GP_USER,
        password=conn.MDES_GP_PASS,
    )

    # 創建游標
    cur = gp_conn.cursor()

    # 執行插入語句
    query = f"INSERT INTO {table_name} ({', '.join(insert_dict.keys())}) VALUES ({', '.join(['%s'] * len(insert_dict))})"
    cur.execute(query, list(insert_dict.values()))

    # 提交事務
    gp_conn.commit()

    # 關閉游標和連線
    cur.close()
    gp_conn.close()


def gp_truncate(table_name):
    gp_conn = psycopg2.connect(
        host=conn.MDES_GP_HOST,
        port=conn.MDES_GP_PORT,
        database=conn.MDES_GP_DB,
        user=conn.MDES_GP_USER,
        password=conn.MDES_GP_PASS,
    )

    # 創建游標
    cur = gp_conn.cursor()

    # 執行 TRUNCATE 語句
    query = f"TRUNCATE TABLE {table_name}"
    cur.execute(query)

    # 提交事務
    gp_conn.commit()

    # 關閉游標和連線
    cur.close()
    gp_conn.close()


def list_s3object(bucket_name, prefix, read_group):
    client = boto3.client(
        "s3",
        endpoint_url=conn.MDES_S3_URL,
        aws_access_key_id=conn.MDES_S3_ACCESS_KEY,
        aws_secret_access_key=conn.MDES_S3_SECRET_KEY,
    )

    # 使用 S3 client 列出物件
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # 過濾出符合擴展名的物件
    allowed_extensions = [".csv", ".xml", ".gz"]
    filtered_objects = [
        obj["Key"]
        for obj in response["Contents"]
        if any(obj["Key"].lower().endswith(ext) for ext in allowed_extensions)
        and obj["Key"].split("/")[1].upper() == read_group
    ]

    return filtered_objects
    # 列印過濾後的物件
    # for key in filtered_objects:
    #     print(key)


def get_redis(r, key):
    result = r.execute_command("JSON.GET", key.decode("utf-8"), ".")
    return result


def get_redis_data(r, key):
    result = r.execute_command("JSON.GET", key.decode("utf-8"), ".data")
    return result


def get_payload(meter_reading, flowfile_attr, file_seqno, file_dir_date, header_dict):
    # ---------------------------------------------------------------------------- #
    #                                    Format                                    #
    # ---------------------------------------------------------------------------- #

    # date format config
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

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

    # ---------------------------------------------------------------------------- #
    #                                   Main Data                                  #
    # ---------------------------------------------------------------------------- #
    MAIN_ENABLE_MK = 0

    # ---------------------------------------------------------------------------- #
    #                               Get Payload Info                               #
    # ---------------------------------------------------------------------------- #
    # Paramter init
    read_time = None
    reading_type = None
    rec_no = None
    meter_id = None
    interval = 0
    note = 0
    meter_id = ""
    fan_id = ""

    # Counter
    columns = {}
    meters = {}
    rt_count = 0
    warn_cnt = 0
    wait_cnt = 0
    err_cnt = 0
    main_succ_cnt = 0

    parsing_time = datetime.now()
    # ------------------------------ 1.Meter Readings ----------------------------- #
    start_strm_time = str(datetime.now())[0:19]
    match meter_reading["Meter"]["Names"]["NameType"]["name"]:
        case "MeterUniqueID":
            meter_id = meter_reading["Meter"]["Names"]["name"]
        case "FanUniqueID":
            fan_id = meter_reading["Meter"]["Names"]["name"]
    if meter_id not in meters.keys():
        meters[meter_id] = 1

    # ------------------------------ 2.Interval Blocks ----------------------------- #
    for interval_block in meter_reading["IntervalBlocks"]:
        read_time = interval_block["IntervalReadings"]["timeStamp"]
        if read_time != None:
            read_time = datetime.strptime(read_time, "%Y-%m-%dT%H:%M:%S.%f%z").strftime(DATE_FORMAT)
        # ------------------------- 臨時程式ID號碼 (TOU_id) 處理台達程式 ------------------------- #
        match interval_block["ReadingType"]["@ref"]:
            case "0.0.0.0.0.2.167.0.0.0.0.0.0.0.0.0.0.0":
                reading_type == "TOU_ID"
                publish_warnlog(
                    flowfile_attr,
                    file_seqno,
                    header_dict["source"],
                    header_dict["read_group"],
                    meter_id,
                    read_time,
                    "W23002",
                    "reading_type",
                    rt_count,
                )
                warn_cnt += 1
            case None:
                publish_warnlog(
                    flowfile_attr,
                    file_seqno,
                    header_dict["source"],
                    header_dict["read_group"],
                    meter_id,
                    read_time,
                    "W24001",
                    "reading_type",
                    rt_count,
                )
                warn_cnt += 1
            case _:
                reading_type = constant.LOADPROFILE[interval_block["ReadingType"]["@ref"]][
                    "name"
                ]  # 讀值欄位
                read_val = interval_block["IntervalReadings"]["value"]
                columns[reading_type] = read_val
                rt_count = len(columns)

        # ---------------------------- 3.Reading Qualities --------------------------- #
        for reading_quality in interval_block["IntervalReadings"]["ReadingQualities"]:
            if header_dict["source"] != "HES-TMAP20210525":
                match reading_quality["ReadingQualityType"]["@ref"]:
                    case "5.4.260":
                        rec_no = reading_quality["comment"]  # 依據Reading Quality判斷是否為rec_no或者讀表狀態
                    case "1.5.257":
                        publish_errorlog(
                            flowfile_attr,
                            file_seqno,
                            header_dict["source"],
                            header_dict["read_group"],
                            meter_id,
                            read_time,
                            "E21001",
                        )
                        err_cnt += 1
                        exitcode = 1
                        sys.exit(exitcode)
                    case _:
                        ref_code = reading_quality["ReadingQualityType"]["@ref"]
                        interval = constant.QUALITYCODE[ref_code]["interval"]
                        note = constant.QUALITYCODE[ref_code]["note"]
            else:
                match reading_quality["ReadingQualityType"]["@ref"]:
                    case "5.4.260":
                        comment = reading_quality["comment"]  # 依據Reading Quality判斷是否為rec_no或者讀表狀態
                    case "1.5.257":
                        publish_errorlog(
                            flowfile_attr,
                            file_seqno,
                            header_dict["source"],
                            header_dict["read_group"],
                            meter_id,
                            read_time,
                            "E21001",
                        )
                        err_cnt += 1
                        exitcode = 1
                        sys.exit(exitcode)
                    case _:
                        ref_code = reading_quality["ReadingQualityType"]["@ref"]
                        interval = constant.QUALITYCODE[ref_code]["interval"]
                        note = constant.QUALITYCODE[ref_code]["note"]

            read_val = interval_block["IntervalReadings"]["value"]
            if reading_type is not None:
                columns[reading_type] = read_val

            del_kwh = columns["DEL_KWH"] if "DEL_KWH" in columns else None
            rec_kwh = columns["REC_KWH"] if "REC_KWH" in columns else None
            del_kvarh_lag = columns["DEL_KVARH_LAG"] if "DEL_KVARH_LAG" in columns else None
            del_kvarh_lead = columns["DEL_KVARH_LEAD"] if "DEL_KVARH_LEAD" in columns else None
            rec_kvarh_lag = columns["REC_KVARH_LAG"] if "REC_KVARH_LAG" in columns else None
            rec_kvarh_lead = columns["REC_KVARH_LEAD"] if "REC_KVARH_LEAD" in columns else None
    print("parsing time: ", datetime.now() - parsing_time)
    if header_dict["source"] != "HES-TMAP20210525":
        lp_raw_temp = {
            "source": header_dict["source"],
            "meter_id": meter_id,
            "fan_id": fan_id,
            "rec_no": rec_no,
            "read_time": read_time,
            "interval": interval,
            "note": note,
            "del_kwh": del_kwh,
            "rec_kwh": rec_kwh,
            "del_kvarh_lag": del_kvarh_lag,
            "del_kvarh_lead": del_kvarh_lead,
            "rec_kvarh_lag": rec_kvarh_lag,
            "rec_kvarh_lead": rec_kvarh_lead,
            "sdp_id": "",
            "ratio": "",
            "pwr_co_id": "",
            "cust_id": "",
            "ct_ratio": "",
            "pt_ratio": "",
            "file_type": flowfile_attr["file_type"],
            "raw_gzfile": flowfile_attr["raw_gzfile"],
            "raw_file": flowfile_attr["raw_file"],
            "rec_time": flowfile_attr["rec_time"],
            "file_path": flowfile_attr["file_path"],
            "file_size": flowfile_attr["file_size"],
            "file_batch_no": flowfile_attr["file_batch_no"],
            "batch_mk": flowfile_attr["batch_mk"],
            "file_seqno": file_seqno,
            "msg_id": header_dict["msg_id"],
            "corr_id": header_dict["corr_id"],
            "msg_time": header_dict["msg_time"],
            "read_group": header_dict["read_group"],
            "verb": header_dict["verb"],
            "noun": header_dict["noun"],
            "context": header_dict["context"],
            "msg_idx": header_dict["msg_idx"],
            "rev": header_dict["rev"],
            "qos": header_dict["qos"],
            "start_strm_time": start_strm_time,
            "warn_dur_ts": "",
            "main_dur_ts": "",
            "rt_count": rt_count,
            "auth_key": "",
            "main_update_time": "",
            "dw_update_time": "",
        }
        lp_raw_temp = check_data(
            lp_raw_temp, flowfile_attr, file_seqno, header_dict["read_group"], rt_count, warn_cnt
        )
    else:
        lpi_raw_temp = {
            "source": header_dict["source"],
            "meter_id": meter_id,
            "fan_id": fan_id,
            "tamp_cust_id": meter_id,  # not sure
            "comment": comment,
            "read_time": read_time,
            "interval": interval,
            "note": note,
            "del_kwh": del_kwh,
            "rec_kwh": rec_kwh,
            "del_kvarh_lag": del_kvarh_lag,
            "del_kvarh_lead": del_kvarh_lead,
            "rec_kvarh_lag": rec_kvarh_lag,
            "rec_kvarh_lead": rec_kvarh_lead,
            "sdp_id": "",
            "ratio": "",
            "pwr_co_id": "",
            "cust_id": "",
            "ct_ratio": "",
            "pt_ratio": "",
            "file_type": flowfile_attr["file_type"],
            "raw_gzfile": flowfile_attr["raw_gzfile"],
            "raw_file": flowfile_attr["raw_file"],
            "rec_time": flowfile_attr["rec_time"],
            "file_path": flowfile_attr["file_path"],
            "file_size": flowfile_attr["file_size"],
            "file_batch_no": flowfile_attr["file_batch_no"],
            "batch_mk": flowfile_attr["batch_mk"],
            "file_seqno": file_seqno,
            "msg_id": header_dict["msg_id"],
            "corr_id": header_dict["corr_id"],
            "msg_time": header_dict["msg_time"],
            "read_group": header_dict["read_group"],
            "verb": header_dict["verb"],
            "noun": header_dict["noun"],
            "context": header_dict["context"],
            "msg_idx": header_dict["msg_idx"],
            "rev": header_dict["rev"],
            "qos": header_dict["qos"],
            "start_strm_time": start_strm_time,
            "warn_dur_ts": "",
            "main_dur_ts": "",
            "rt_count": rt_count,
            "auth_key": "",
            "main_update_time": "",
            "dw_update_time": "",
        }
        lpi_raw_temp, warn_cnt = check_data(
            lpi_raw_temp, flowfile_attr, file_seqno, header_dict["read_group"], rt_count, warn_cnt
        )

    # ---------------------------------------------------------------------------- #
    #                             lp_raw with main_data                            #
    # ---------------------------------------------------------------------------- #
    if MAIN_ENABLE_MK == 1:
        main_start_time = datetime.now()
        if read_time is not None:
            read_time_ux = int(datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp())
        main_srch = redis_conn.execute_command(
            "ft.search",
            "maindata_idx",
            f"@meter:{{{meter_id}}}",
            "RETURN",
            "2",
            "$.sdp_id",
            f"$.data[?(@.begin<={read_time_ux}&&@.end>{read_time_ux})]",
        )

        if main_srch[0] == 1:  # 假設有主檔 會回傳一個list包含[1(主檔比數), maindata_idx, [$.sdp_id,$.date]]
            read_time_int = int(datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp())
            if header_dict["source"] != "HES-TMAP20210525":
                combine_maindata(main_srch, lp_raw_temp, main_start_time)
                main_succ_cnt += 1
                publish_kafka(
                    lp_raw_temp,
                    "mdes.stream.lp-raw",
                    hash_func(meter_id),
                )
            else:
                combine_maindata(main_srch, lpi_raw_temp, main_start_time)
                main_succ_cnt += 1
                publish_kafka(
                    lpi_raw_temp,
                    "mdes.stream.lpi-raw",
                    hash_func(meter_id),
                )
        else:
            wait_cnt += 1
            if main_srch[0] > 1:
                publish_errorlog(
                    flowfile_attr,
                    file_seqno,
                    header_dict["source"],
                    header_dict["read_group"],
                    meter_id,
                    read_time,
                    "E23003",
                )
                warn_cnt += 1
            main_dur_ts = (datetime.now() - main_start_time).microseconds
            read_time_int = int(datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp())

            if header_dict["source"] != "HES-TMAP20210525":
                set_nomaindata(
                    lp_raw_temp,
                    main_dur_ts,
                    read_time_int,
                    meter_id,
                    file_dir_date,
                    redis_conn,
                )
            else:
                set_nomaindata(
                    lpi_raw_temp,
                    main_dur_ts,
                    read_time_int,
                    meter_id,
                    file_dir_date,
                    redis_conn,
                )
    else:
        if header_dict["source"] != "HES-TMAP20210525":
            publish_kafka_v2(
                lp_raw_temp,
                "mdes.stream.lp-raw",
                hash_func(meter_id),
            )
        else:
            publish_kafka_v2(
                lpi_raw_temp,
                "mdes.stream.lpi-raw",
                hash_func(meter_id),
            )
    # kafka_flush()
    return meters, warn_cnt, err_cnt, wait_cnt, main_succ_cnt


def count_total(input_array):
    meters_count = {}  # 用於統計不同meters 的數量
    total_meter_cnt = 0

    warn_cnt_total = 0
    err_cnt_total = 0
    wait_cnt_total = 0
    main_succ_cnt_total = 0

    for item in input_array:
        meters, warn_cnt, err_cnt, wait_cnt, main_succ_cnt = item

        # 統計不同meters 的數量
        for meter, count in meters.items():
            meters_count[meter] = meters_count.get(meter, 0) + count

        warn_cnt_total += warn_cnt
        err_cnt_total += err_cnt
        wait_cnt_total += wait_cnt
        main_succ_cnt_total += main_succ_cnt

    print("Meters Count:")
    for meter, count in meters_count.items():
        total_meter_cnt += count
        print(f"{meter}: {count}")

    print("Total Counts:")
    print(f"meter_cnt_total: {total_meter_cnt}")
    print(f"warn_cnt_total: {warn_cnt_total}")
    print(f"err_cnt_total: {err_cnt_total}")
    print(f"wait_cnt_total: {wait_cnt_total}")
    print(f"main_succ_cnt_total: {main_succ_cnt_total}")

    return total_meter_cnt, warn_cnt_total, err_cnt_total, wait_cnt_total, main_succ_cnt_total


def count_total_v2(input_array):
    meters_count = {}  # 用於統計不同meters 的數量
    total_meter_cnt = 0

    warn_cnt_total = 0
    err_cnt_total = 0
    wait_cnt_total = 0
    main_succ_cnt_total = 0

    meters, warn_cnt, err_cnt, wait_cnt, main_succ_cnt = input_array

    # 統計不同meters 的數量
    for meter, count in meters.items():
        meters_count[meter] = meters_count.get(meter, 0) + count

    warn_cnt_total += warn_cnt
    err_cnt_total += err_cnt
    wait_cnt_total += wait_cnt
    main_succ_cnt_total += main_succ_cnt

    print("Meters Count:")
    for meter, count in meters_count.items():
        total_meter_cnt += count
        print(f"{meter}: {count}")

    print("Total Counts:")
    print(f"meter_cnt_total: {total_meter_cnt}")
    print(f"warn_cnt_total: {warn_cnt_total}")
    print(f"err_cnt_total: {err_cnt_total}")
    print(f"wait_cnt_total: {wait_cnt_total}")
    print(f"main_succ_cnt_total: {main_succ_cnt_total}")

    return total_meter_cnt, warn_cnt_total, err_cnt_total, wait_cnt_total, main_succ_cnt_total
