"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) TMAP高壓用戶讀表插補作業
"""
# Author：JamesChen
# Date：2023/O6/06
#
# Modified by：[V0.01][20230531][JamesChen][]

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from os.path import dirname
from typing import Any, Dict

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

    try:
        flowfile_json = sys.stdin.buffer.read().decode("utf-8")
        flowfile_data = json.loads(flowfile_json)

        start_strm_time = datetime.now()
        read_time = flowfile_data["read_time"]
        meter_id = flowfile_data["meter_id"]
        read_date = flowfile_data["read_date"]

        prev_read_time = int(
            (datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S") - timedelta(hours=24)).timestamp()
        )  # 取得前 24 小時的時間並轉換為 Unix 時間戳記

        next_read_time = int(
            (datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S") + timedelta(hours=24)).timestamp()
        )  # 取得後 24 小時的時間並轉換為 Unix 時間戳記

        curr_read_time = int(
            datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").timestamp()
        )  # read_time轉換為 Unix 時間戳記

        prev_date = (
            datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S") - timedelta(hours=24)
        ).strftime(
            "%Y-%m-%d"
        )  # 取得前 24 小時的時間並將其轉換為日期字串（YYYY-MM-DD）

        next_date = (
            datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S") + timedelta(hours=24)
        ).strftime(
            "%Y-%m-%d"
        )  # 取得後 24 小時的時間並將其轉換為日期字串（YYYY-MM-DD）

        curr_date = datetime.strptime(read_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")

        ref_start_time = datetime.now()

        range_list = [
            {"read_date": prev_date, "read_time_int": prev_read_time, "proc_type": 1},
            {"read_date": curr_date, "read_time_int": curr_read_time, "proc_type": 2},
            {"read_date": next_date, "read_time_int": next_read_time, "proc_type": 3},
        ]

        for item in range_list:
            read_date = item["read_date"]
            read_time_int = item["read_time_int"]
            proc_type = item["proc_type"]

            lp_data = []

            match proc_type:
                case 1:
                    cmd1 = "lp_data:" + meter_id + "_" + read_date
                    cmd2 = "$.data[?(@interval!=4&&@read_time_int>=" + str(read_time_int) + ")]"
                case 2:
                    cmd1 = "lp_data:" + meter_id + "_" + read_date
                    cmd2 = "$.data[?(@interval!=4)]"
                case 3:
                    cmd1 = "lp_data:" + meter_id + "_" + read_date
                    cmd2 = "$.data[?(@interval!=4&&@read_time_int<=" + str(read_time_int) + ")]"

            result = redis_conn.execute_command("JSON.GET", cmd1, cmd2)
            if result is not None:
                lp_data += json.loads(result)
        lp_data_df = pd.DataFrame(
            lp_data,
            columns=[
                "source",
                "meter_id",
                "fan_id",
                "rec_no",
                "read_time",
                "read_time_int",
                "read_time_bias",
                "interval",
                "note",
                "del_kwh",
                "del_kvarh_lag",
                "version",
                "proc_type",
                "sdp_id",
                "ratio",
                "pwr_co_id",
                "cust_id",
                "ct_ratio",
                "pt_ratio",
                "file_type",
                "raw_gzfile",
                "raw_file",
                "rec_time",
                "file_path",
                "file_size",
                "file_seqno",
                "msg_id",
                "corr_id",
                "msg_time",
                "rev",
                "qos",
                "start_strm_time",
                "warn_dur_ts",
                "main_dur_ts",
                "dedup_dur_ts",
                "error_dur_ts",
                "dup_dur_ts",
                "hist_dur_ts",
                "hist_mk",
                "ref_dur_ts",
                "imp_dur_ts",
                "out_dur_ts",
                "out_mk",
                "ver_dur_ts",
                "end_strm_time",
                "rt_count",
                "part_no",
                "main_update_time",
                "dw_update_time",
            ],
        )
        ref_dur_ts = str(datetime.now() - ref_start_time)

        lp_data_df.drop(
            [
                "del_kvarh_lag",
                "version",
                "proc_type",
                "sdp_id",
                "ratio",
                "pwr_co_id",
                "cust_id",
                "ct_ratio",
                "pt_ratio",
                "file_type",
                "raw_gzfile",
                "raw_file",
                "rec_time",
                "file_path",
                "file_size",
                "file_seqno",
                "msg_id",
                "corr_id",
                "msg_time",
                "rev",
                "qos",
                "start_strm_time",
                "warn_dur_ts",
                "main_dur_ts",
                "dedup_dur_ts",
                "error_dur_ts",
                "dup_dur_ts",
                "hist_dur_ts",
                "hist_mk",
                "ref_dur_ts",
                "imp_dur_ts",
                "out_dur_ts",
                "out_mk",
                "ver_dur_ts",
                "end_strm_time",
                "rt_count",
                "part_no",
                "main_update_time",
                "dw_update_time",
            ],
            axis=1,
            inplace=True,
        )
        # 依據read_time asc 排序
        lp_data_df.sort_values(by="read_time", ascending=True, inplace=True)

        lp_data_df["prev_read_time"] = lp_data_df["read_time_int"].shift(1).fillna(0).astype(int)
        lp_data_df["next_read_time"] = lp_data_df["read_time_int"].shift(-1).fillna(0).astype(int)

        prev_read_time = lp_data_df[lp_data_df["read_time_int"] == curr_read_time][
            "prev_read_time"
        ].values[0]

        if prev_read_time == 0:
            flowfile_data["hist_mk"] = 1
            func.publish_kafka(flowfile_data, "mdes.stream.lpi-hist", meter_id)
        # 該筆串流值目前無缺值
        elif (lp_data_df["next_read_time"] - lp_data_df["prev_read_time"]) == 172800:
            pass
        else:
            imp_start_time = datetime.now()

            # 篩選 僅保留當筆串流值和其前、後一筆實際值
            next_read_time = lp_data_df[lp_data_df["read_time_int"] == curr_read_time][
                "next_read_time"
            ].values[0]

            print(prev_read_time, next_read_time)
            filter_df = lp_data_df.loc[
                (lp_data_df["read_time_int"] == prev_read_time)
                | (lp_data_df["read_time_int"] == next_read_time)
                | (lp_data_df["read_time_int"] == curr_read_time)
            ]

            # 插補 15分鐘區間
            filter_df["read_time"] = pd.to_datetime(filter_df["read_time"])
            filter_df.index = filter_df["read_time"]

            impute = filter_df.groupby("meter_id").resample("15T").mean()
            impute["interval"] = impute["interval"].fillna(value=4)
            impute["note"] = impute["note"].fillna(value=1)
            impute["version"] = impute["version"].fillna(value=1)
            impute["proc_type"] = impute["proc_type"].fillna(value="S1001")

            # 取前一筆實際值
            impute["source"] = impute["source"].fillna(method="ffill")
            impute["meter_id"] = impute["meter_id"].fillna(method="ffill")
            impute["fan_id"] = impute["fan_id"].fillna(method="ffill")
            impute["rec_no"] = impute["rec_no"].fillna(method="ffill")
            impute["read_time"] = impute["read_time"].fillna(method="ffill")
            impute["read_time_bias"] = impute["read_time_bias"].fillna(method="ffill")

            # 預設為null
            impute["del_kwh"] = impute["del_kwh"].fillna(value=None)
            impute["rec_kwh"] = impute["rec_kwh"].fillna(value=None)
            impute["del_kvarh_lag"] = impute["del_kvarh_lag"].fillna(value=None)
            impute["del_kvarh_lead"] = impute["del_kvarh_lead"].fillna(value=None)
            impute["rec_kvarh_lag"] = impute["rec_kvarh_lag"].fillna(value=None)
            impute["rec_kvarh_lead"] = impute["rec_kvarh_lead"].fillna(value=None)
            impute["file_type"] = impute["file_type"].fillna(value=None)
            impute["raw_gzfile"] = impute["raw_gzfile"].fillna(value=None)
            impute["raw_file"] = impute["raw_file"].fillna(value=None)
            impute["rec_time"] = impute["rec_time"].fillna(value=None)
            impute["file_path"] = impute["file_path"].fillna(value=None)
            impute["file_size"] = impute["file_size"].fillna(value=None)
            impute["file_seqno"] = impute["file_seqno"].fillna(value=None)
            impute["msg_id"] = impute["msg_id"].fillna(value=None)
            impute["corr_id"] = impute["corr_id"].fillna(value=None)
            impute["msg_time"] = impute["msg_time"].fillna(value=None)
            impute["rev"] = impute["rev"].fillna(value=None)
            impute["qos"] = impute["qos"].fillna(value=None)

            # start_strm_time
            impute["start_strm_time"] = start_strm_time

            # 預設0
            impute["warn_dur_ts"] = impute["warn_dur_ts"].fillna(value=0)
            impute["dedup_dur_ts"] = impute["dedup_dur_ts"].fillna(value=0)
            impute["error_dur_ts"] = impute["error_dur_ts"].fillna(value=0)
            impute["dup_dur_ts"] = impute["dup_dur_ts"].fillna(value=0)
            impute["hist_dur_ts"] = impute["hist_dur_ts"].fillna(value=0)
            impute["hist_mk"] = impute["hist_mk"].fillna(value=0)
            impute["main_dur_ts"] = impute["main_dur_ts"].fillna(value=0)
            impute["out_dur_ts"] = impute["out_dur_ts"].fillna(value=0)
            impute["out_mk"] = impute["out_mk"].fillna(value=0)

            # ref_dur_ts
            impute["ref_dur_ts"] = ref_dur_ts

            # imp_dur_ts
            impute["imp_dur_ts"] = str(datetime.now() - imp_start_time)

            # imp_mk
            impute["imp_mk"] = 1

            # 取前一筆實際值
            impute["sdp_id"] = impute["sdp_id"].fillna(method="ffill")
            impute["ratio"] = impute["ratio"].fillna(method="ffill")
            impute["pwr_co_id"] = impute["pwr_co_id"].fillna(method="ffill")
            impute["cust_id"] = impute["cust_id"].fillna(method="ffill")
            impute["ct_ratio"] = impute["ct_ratio"].fillna(method="ffill")
            impute["pt_ratio"] = impute["pt_ratio"].fillna(method="ffill")
            impute["tmap_cust_id"] = impute["tmap_cust_id"].fillna(method="ffill")

            # 預設0
            impute["comment"] = impute["comment"].fillna(value=0)

            # 取前一筆實際值
            impute["part_no"] = impute["part_no"].fillna(method="ffill")
            impute["rt_count"] = impute["rt_count"].fillna(method="ffill")

            print(impute)

            ver_start_time = datetime.now()
            impute = impute[impute["interval"] == 4]

            for index, row in impute.iterrows():
                read_time_int = row["read_time_int"]
                read_date = datetime.strptime(row["read_time"], DATE_FORMAT).strftime("%Y-%m-%d")
                meter_id = row["meter_id"]
                lpdata_key = "lp_data:" + meter_id + "_" + read_date
                lp_result = redis_conn.execute_command(
                    "JSON.GET",
                    lpdata_key,
                    '$.data[?(@.read_time_int=={0}&&@.meter_id=="{1}")]'.format(
                        read_time_int, meter_id
                    ),
                )
                if lp_result is not None:
                    continue
                else:
                    selected_row = impute.loc[index]
                    selected_row["ver_dur_time"] = str(datetime.now()-ver_start_time)
                    json_data = selected_row.to_json()
                    redis_conn.execute_command("JSON.SET", lpdata_key, ".", json_data)
                    func.publish_kafka(json_data, "mdes.stream.lpi", meter_id)

    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_TmapProcess",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
