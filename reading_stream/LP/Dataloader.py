"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 將主資料庫讀表值(greenplum)載入至Redis暫時資料庫
  (2) 更新待處理載入清單狀態
"""
# Author：JamesChen
# Date：2023/O6/09
#
# Modified by：[V0.01][20230531][JamesChen][]

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from os.path import dirname

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
        # ---------------------------------------------------------------------------- #
        #                               Flowfile Content                               #
        # ---------------------------------------------------------------------------- #

        # 讀取Flowfile內容
        flowfile_json = sys.stdin.buffer.read().decode("utf-8")
        flowfile_data = json.loads(flowfile_json)

        union_queries_lpr = ""
        union_queries_lpi = ""
        for index, data in enumerate(flowfile_data):
            ref_batch_no = data["ref_batch_no"]
            read_group = data["read_group"]
            source = data["source"]
            meter_id = data["meter_id"]
            start_read_time = datetime.fromtimestamp(int(data["start_read_time"]) / 1000)
            end_read_time = datetime.fromtimestamp(int(data["end_read_time"]) / 1000)
            proc_type = data["proc_type"]
            expiry_date = data["expiry_date"]
            # read_time = data["read_time"]

            update_values = {"proc_type": 1}

            condition_values = {"ref_batch_no": ref_batch_no, "meter_id": meter_id}

            # 依據前步驟取得之ref_batch_no更新各筆訂單狀態proc_type=1
            func.gp_update_v2("ami_dg.data_hist_ref_list", update_values, condition_values)

            if read_group == "LP":
                sub_query = f"""
                    SELECT
                        *
                    FROM
                        ami_ods.lpr
                    WHERE
                        meter_id = '{meter_id}'
                        AND source = '{source}'
                        AND read_time >= '{start_read_time}' :: timestamp - interval '1 days'
                        AND read_time <= '{end_read_time}' :: timestamp + interval '1 days'
                """

                # 將 union 子查詢合併成一個完整的 SQL 查詢
                # 判斷是否為最後一個迴圈
                if index < len(flowfile_data) - 1:
                    sub_query += " UNION ALL"

                union_queries_lpr += sub_query
            else:
                sub_query = f"""
                    SELECT
                        *
                    FROM
                        ami_ods.lpi
                    WHERE
                        meter_id = '{meter_id}'
                        AND source = '{source}'
                        AND read_time >= '{start_read_time}' :: timestamp - interval '1 days'
                        AND read_time <= '{end_read_time}' :: timestamp + interval '1 days'
                """

                # 將 union 子查詢合併成一個完整的 SQL 查詢
                # 判斷是否為最後一個迴圈
                if index < len(flowfile_data) - 1:
                    sub_query += " UNION ALL"

                union_queries_lpi += sub_query

        select_query = f"""
            SELECT
                source,
                meter_id,
                fan_id,
                rec_no,
                read_time,
                read_time_bias,
                interval,
                note,
                del_kwh,
                rec_kwh,
                del_kvarh_lag,
                del_kvarh_lead,
                rec_kvarh_lag,
                rec_kvarh_lead,
                version,
                proc_type,
                sdp_id,
                ratio,
                pwr_co_id,
                cust_id,
                ct_ratio,
                pt_ratio,
                file_type,
                raw_gzfile,
                raw_file,
                rec_time,
                file_path,
                file_size,
                file_seqno,
                msg_id,
                corr_id,
                msg_time,
                verb,
                noun,
                context,
                msg_idx,
                rev,
                qos,
                start_strm_time,
                warn_dur_ts,
                main_dur_ts,
                dedup_dur_ts,
                error_dur_ts,
                dup_dur_ts,
                hist_dur_ts,
                hist_mk,
                ref_dur_ts,
                imp_dur_ts,
                imp_mk,
                out_dur_ts,
                out_mk,
                ver_dur_ts,
                end_strm_time,
                rt_count,
                part_no,
                auth_key,
                main_update_time,
                dw_update_time
            FROM
                (
        """

        hist_start_time = datetime.now()
        reflog_key = "lp_hist_reflog:" + meter_id + "_" + ref_batch_no
        # 自主資料庫查詢如下(需將所有訂單之資訊整併成單一request(union all)
        if read_group == "LP":
            union_search_lpr = select_query + union_queries_lpr + ") AS union_queries_lpr;"
            union_result_lpr = func.gp_search(union_search_lpr)
            hist_dur_ts = str(datetime.now() - hist_start_time)

            if len(union_result_lpr) > 0:
                # 將上述回傳資料結果新增更新至Redis lp_data:{key}
                read_time = union_search_lpr[0][4]
                lpdata_key = "lp_data:" + meter_id + "_" + datetime.strptime(read_time, DATE_FORMAT)
                func.set_redis(redis_conn, lpdata_key, union_result_lpr)
                redis_conn.execute_command("EXPIRE", lpdata_key, 950400)
                func.set_redis_data(redis_conn, lpdata_key, {"hist_dur_ts": hist_dur_ts})
                func.set_redis_data(redis_conn, reflog_key, {"hist_dur_ts": hist_dur_ts})
        else:
            union_search_lpi = select_query + union_queries_lpi + ");"
            union_result_lpi = func.gp_search(union_search_lpi)
            hist_dur_ts = str(datetime.now() - hist_start_time)

            if len(union_result_lpi) > 0:
                # 將上述回傳資料結果新增更新至Redis lpi_data:{key}
                read_time = union_search_lpi[0][4]
                lpidata_key = (
                    "lpi_data:" + meter_id + "_" + datetime.strptime(read_time, DATE_FORMAT)
                )
                func.set_redis(redis_conn, lpidata_key, union_result_lpi)
                redis_conn.execute_command("EXPIRE", lpidata_key, 950400)
                func.set_redis_data(redis_conn, lpidata_key, {"hist_dur_ts": hist_dur_ts})
                func.set_redis_data(redis_conn, reflog_key, {"hist_dur_ts": hist_dur_ts})

        for data in flowfile_data:
            ref_batch_no = data["ref_batch_no"]
            print(ref_batch_no)
            read_group = data["read_group"]
            meter_id = data["meter_id"]
            reflog_key = "lp_hist_reflog:" + meter_id + "_" + ref_batch_no
            print(reflog_key)
            # 依據前述步驟取得之所有ref_batch_no更新各筆訂單狀態proc_type=2
            update_values = {"proc_type": 2}
            condition_values = {"ref_batch_no": ref_batch_no}
            func.gp_update_v2("ami_dg.data_hist_ref_list", update_values, condition_values)

            # 自lp_hist_reflog:{key}取得待處理之讀表記錄
            hist_result = redis_conn.execute_command(
                "FT.SEARCH lp_hist_reflog_idx @ref_batch_no:{ref_batch_no} RETURN 1 $.data".format(
                    ref_batch_no=ref_batch_no
                )
            )
            print(hist_result)

            # 將資料轉拋至Topic
            if read_group == "LP":
                func.publish_kafka(hist_result, "mdes.stream.lpr-load", func.hash_func(meter_id))
            else:
                func.publish_kafka(hist_result, "mdes.stream.lpi-load", func.hash_func(meter_id))

            # 刪除該筆讀值於lp_hist_reflog:{key}的紀錄
            redis_conn.delete(reflog_key)
    except Exception as e:
        logging.error(
            "Processor Group: {%s}, Process: {%s}",
            "meterreadings_lp_stream",
            "LP_Dataload",
            exc_info=True,
        )
        print(e)
        exitcode = 1
    finally:
        sys.exit(exitcode)
