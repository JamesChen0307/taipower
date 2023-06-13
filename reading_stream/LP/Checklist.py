"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) 檢閱data_hist_ref_list是否需要data load
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


def _datahist_search(source, meter_id, read_group, read_time):
    datahist_ref = func.gp_search(
        """
                SELECT
                *
                FROM
                (
                    SELECT
                        ref_batch_no,
                        read_group,
                        source,
                        meter_id,
                        start_read_time,
                        end_read_time,
                        ROW_NUMBER() OVER (
                            PARTITION BY source,
                            meter_id
                            ORDER BY
                            crtd_time,
                            channel_type DESC
                        ) AS idx,
                        channel_type,
                        proc_type,
                        expiry_date,
                        current_date
                    FROM
                    (
                        SELECT
                            ref_batch_no,
                            read_group,
                            source,
                            meter_id,
                            start_read_time,
                            end_read_time,
                            proc_type,
                            expiry_date,
                            crtd_time,
                            CASE
                                WHEN proc_type = 0 THEN 1
                                WHEN proc_type = 1 THEN 2
                                WHEN proc_type = 2
                                AND expiry_date > current_date THEN 3 --not due yet
                            END AS channel_type
                        FROM
                            ami_dg.data_hist_ref_list
                        WHERE
                            source = '{source}'
                        AND meter_id = '{meter_id}'
                        AND read_group = '{read_group}'
                        AND NOT (
                            proc_type = 2
                            AND expiry_date < current_date
                        )
                        AND start_read_time <= '{read_time}'
                        AND end_read_time >= '{read_time}'
                    ) a
                ) b
                WHERE
                    idx = 1;
        """.format(
            source=source, meter_id=meter_id, read_group=read_group, read_time=read_time
        )
    )
    return datahist_ref


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

        read_time = flowfile_data["read_time"]
        meter_id = flowfile_data["meter_id"]
        source = flowfile_data["source"]
        read_group = flowfile_data["read_group"]
        src_flow = flowfile_data["src_flow"]

        read_time_int = int(datetime.strptime(flowfile_data["read_time"], DATE_FORMAT).timestamp())

        datahist_ref = _datahist_search(source, meter_id, read_group, read_time)
        # 查詢結果無值，表示目前沒有關於此筆串流的待處理紀錄
        if len(datahist_ref) == 0:
            # 檢查是否有該meter_id且proc_type=0且既有區間與本筆串流值為同日之訂單
            check_result = func.gp_search(
                """
                SELECT
                    ref_batch_no,
                    read_group,
                    source,
                    meter_id,
                    start_read_time,
                    end_read_time,
                    expiry_date,
                    proc_type,
                    req_strm_cnt
                FROM
                    ami_dg.data_hist_ref_list
                WHERE
                    source = '{source}'
                    AND meter_id = '{meter_id}'
                    AND read_group = '{read_group}'
                    AND proc_type = 0
                    AND (
                        start_read_date = DATE_TRUNC('day', '{read_time}' :: timestamp)
                        OR end_read_date = DATE_TRUNC('day', '{read_time}' :: timestamp)
                    )
                ORDER BY
                    crtd_time
                LIMIT
                    1
                ;
            """.format(
                    source=source, meter_id=meter_id, read_group=read_group, read_time=read_time
                )
            )

            # 查詢結果有尚未處理(proc_type=0)且相同meter_id的訂單
            # 以上述結果之ref_batch_no更新該筆訂單之start_read_time、end_read_time、req_strm_cnt
            if check_result:
                ref_batch_no = check_result["ref_batch_no"]
                if check_result["start_read_time"] > read_time:
                    func.gp_update(
                        "UPDATE ami_dg.data_hist_ref_list SET start_read_time = %s WHERE ref_batch_no = %s",
                        read_time,
                        ref_batch_no,
                    )
                if check_result["end_read_time"] < read_time:
                    func.gp_update(
                        "UPDATE ami_dg.data_hist_ref_list SET end_read_time = %s WHERE ref_batch_no = %s",
                        read_time,
                        ref_batch_no,
                    )
                func.gp_update(
                    "UPDATE ami_dg.data_hist_ref_list SET req_strm_cnt = %s WHERE ref_batch_no = %s",
                    check_result["req_strm_cnt"] + 1,
                    ref_batch_no,
                )

                # 保留當筆串流值至Redis待處理資料載入紀錄
                reflog_key = "lp_hist_reflog:" + meter_id + "_" + ref_batch_no
                reflog_result = redis_conn.execute_command(
                    "JSON.GET",
                    reflog_key,
                    "$.data[?(@.read_time_int=={0})]".format(read_time_int),
                )
                if reflog_result:
                    redis_conn.execute_command(
                        "JSON.SET", reflog_key, ".data", json.dumps(flowfile_data)
                    )
                else:
                    redis_conn.execute_command(
                        "JSON.ARRAPPEND", reflog_key, ".data", json.dumps(flowfile_data)
                    )

            # 查詢結果無值，表示目前沒有關於此筆串流的待處理紀錄，且沒有步驟3的同日未處理清單
            else:
                batch_no_tmp = datetime.now().strftime("%Y%m%d%H%M%S")
                srch_str = """
                    SELECT
                        CASE
                            WHEN COUNT(*) > 0 THEN MAX(ref_seqno) + 1
                            ELSE 1
                        END AS new_ref_seqno
                    FROM
                        ami_dg.data_hist_ref_list
                    WHERE
                        ref_batch_no LIKE '{ref_batch_no}%';
                """.format(
                    ref_batch_no=batch_no_tmp
                )
                ref_seqno = func.gp_search(srch_str[0])
                ref_batch_no = batch_no_tmp + str(ref_seqno)
                ref_dict = {
                    "ref_seqno": ref_seqno,
                    "ref_batch_no": ref_batch_no,
                    "start_read_time": read_time,
                    "end_read_time": read_time,
                    "start_read_date": datetime.strptime(read_time, DATE_FORMAT).strftime(
                        "%Y-%m-%d"
                    ),
                    "end_read_date": datetime.strptime(read_time, DATE_FORMAT).strftime("%Y-%m-%d"),
                    "req_strm_cnt": 1,
                    "proc_type": 0,
                    "crtd_time": datetime.now().strftime(DATE_FORMAT),
                }

                # 新增一筆訂單至主資料庫data_hist_ref_list
                func.gp_insert("ami_dg.data_hist_ref_list", ref_dict)

                reflog_key = "lp_hist_reflog:" + meter_id + "_" + ref_batch_no
                reflog_result = redis_conn.execute_command(
                    "JSON.GET",
                    reflog_key,
                    "$.data[?(@.read_time_int=={0})]".format(read_time_int),
                )

                # 保留當筆串流值至Redis待處理資料載入紀錄lp_hist_reflog:{key}
                if reflog_result:
                    redis_conn.execute_command(
                        "JSON.SET", reflog_key, ".data", json.dumps(flowfile_data)
                    )
                else:
                    redis_conn.execute_command(
                        "JSON.ARRAPPEND", reflog_key, ".data", json.dumps(flowfile_data)
                    )

        # 查詢結果有值
        else:
            match datahist_ref[0][8]:  # channel_type
                # 且回傳之channel_type=1(proc_type=0)，表示已有訂單，但尚未處理
                case 1:
                    reflog_key = (
                        "lp_hist_reflog:" + meter_id + "_" + datahist_ref[0][0]
                    )  # ref_batch_no
                    reflog_result = redis_conn.execute_command(
                        "JSON.GET",
                        reflog_key,
                        "$.data[?(@.read_time_int=={0})]".format(read_time_int),
                    )

                    # 保留當筆串流值至Redis待處理資料載入紀錄lp_hist_reflog:{key}
                    if reflog_result:
                        redis_conn.execute_command(
                            "JSON.SET", reflog_key, ".data", json.dumps(flowfile_data)
                        )
                    else:
                        redis_conn.execute_command(
                            "JSON.ARRAPPEND", reflog_key, ".data", json.dumps(flowfile_data)
                        )

                # 且回傳之channel_type=2，表示資料載入中，故需等待(while loop sleep(30秒))再進行檢查
                # 如等待大於1日則記錄至ami_dg.lp_hist_error_log，並結束本處理程序
                # 若處理完成且channel_type=3則執行下一步驟
                case 2:
                    start_time = datetime.now()
                    target_time = start_time + timedelta(days=1)
                    while True:
                        current_time = datetime.now()
                        if current_time > target_time:
                            # 記錄錯誤至 ami_dg.lp_hist_error_log 表中
                            func.gp_insert("ami_dg.lp_hist_error_log", flowfile_data)
                            break
                        re_search = _datahist_search(source, meter_id, read_group, read_time)
                        if isinstance(re_search, list) and re_search:
                            if re_search[0][8] == 3:
                                break
                        # 暫停 30 秒
                        time.sleep(30)

                # 且回傳之channel_type=3
                # 將資訊轉拋至mdes.stream.lpr-load或mdes.stream.lpi-load後結束本處理程序
                case 3:
                    if source != "HES-TMAP20210525":
                        func.publish_kafka(
                            flowfile_data, "mdes.stream.lpr-load", func.hash_func(meter_id)
                        )
                    else:
                        func.publish_kafka(
                            flowfile_data, "mdes.stream.lpi-load", func.hash_func(meter_id)
                        )

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
