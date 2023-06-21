"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) Load Profile 將歷史參考資料載入至Redis，並記錄載入統計資訊至ami_dg.data_hist_stat
"""

# Author：JamesChen
# Date：2023/O6/19
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#

import json
import logging
import os
import sys
from datetime import datetime, timedelta, time
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
            # 檢查有無執行中的作業
            result_jobcnt = func.gp_search(
                "select count(*) as jobcnt from ami_dg.bucket_ctrl_log where proc_type = -1 and read_group='LP';"
            )

            jobcnt = result_jobcnt[0][0]
            print(jobcnt)

            # 表示上一個作業未完成，需等待
            if jobcnt > 1:
                exitcode = 1
                sys.exit(exitcode)
            else:
                bucket_crtl = func.gp_search(
                    """
                        SELECT
                            file_batch_no,
                            start_date
                        FROM
                            ami_dg.bucket_ctrl_log
                        WHERE
                            proc_type = -2
                            AND enable_mk = 1
                            AND read_group = 'LP'
                        ORDER BY
                            crtd_time asc
                        LIMIT
                            1;
                    """
                )
                print(bucket_crtl)

                file_batch_no = bucket_crtl[0][0]
                start_date = bucket_crtl[0][1]

                # 表示處理中
                func.gp_update(
                    "UPDATE ami_dg.bucket_ctrl_log SET proc_type = %s WHERE file_batch_no = %s;",
                    -1,
                    file_batch_no,
                )

                read_start_time = (start_date - timedelta(days=7) + timedelta(minutes=15)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                read_end_time = datetime.combine(
                    (start_date + timedelta(days=1)), time(0, 0)
                ).strftime("%Y-%m-%dT%H:%M:%S")

                lpr_result = func.gp_search(
                    """
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
                            ami_ods.lpr
                        WHERE
                            read_time >= '{read_start_time}'
                            AND read_time <= '{read_end_time}';
                    """
                )

                lpi_result = func.gp_search(
                    """
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
                            tmap_cust_id,
                            comment,
                            auth_key,
                            main_update_time,
                            dw_update_time
                        FROM
                            ami_ods.lpr
                        WHERE
                            read_time >= '{read_start_time}'
                            AND read_time <= '{read_end_time}';
                    """
                )

                lpr_keys = [
                    "source",
                    "meter_id",
                    "fan_id",
                    "rec_no",
                    "read_time",
                    "read_time_bias",
                    "interval",
                    "note",
                    "del_kwh",
                    "rec_kwh",
                    "del_kvarh_lag",
                    "del_kvarh_lead",
                    "rec_kvarh_lag",
                    "rec_kvarh_lead",
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
                    "verb",
                    "noun",
                    "context",
                    "msg_idx",
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
                    "imp_mk",
                    "out_dur_ts",
                    "out_mk",
                    "ver_dur_ts",
                    "end_strm_time",
                    "rt_count",
                    "part_no",
                    "auth_key",
                    "main_update_time",
                    "dw_update_time",
                ]

                lpi_keys = [
                    'source',
                    'meter_id',
                    'fan_id',
                    'rec_no',
                    'read_time',
                    'read_time_bias',
                    'interval',
                    'note',
                    'del_kwh',
                    'rec_kwh',
                    'del_kvarh_lag',
                    'del_kvarh_lead',
                    'rec_kvarh_lag',
                    'rec_kvarh_lead',
                    'version',
                    'proc_type',
                    'sdp_id',
                    'ratio',
                    'pwr_co_id',
                    'cust_id',
                    'ct_ratio',
                    'pt_ratio',
                    'file_type',
                    'raw_gzfile',
                    'raw_file',
                    'rec_time',
                    'file_path',
                    'file_size',
                    'file_seqno',
                    'msg_id',
                    'corr_id',
                    'msg_time',
                    'verb',
                    'noun',
                    'context',
                    'msg_idx',
                    'rev',
                    'qos',
                    'start_strm_time',
                    'warn_dur_ts',
                    'main_dur_ts',
                    'dedup_dur_ts',
                    'error_dur_ts',
                    'dup_dur_ts',
                    'hist_dur_ts',
                    'hist_mk',
                    'ref_dur_ts',
                    'imp_dur_ts',
                    'imp_mk',
                    'out_dur_ts',
                    'out_mk',
                    'ver_dur_ts',
                    'end_strm_time',
                    'rt_count',
                    'part_no',
                    'tmap_cust_id',
                    'comment',
                    'auth_key',
                    'main_update_time',
                    'dw_update_time'
                ]

                lp_data = []
                lpi_data = []
                for item in lpr_result:
                    item_dict = dict(zip(lpr_keys, item))
                    item_dict["msg_time_int"] = int(datetime.strptime(item["msg_time"], DATE_FORMAT).timestamp())
                    item_dict["rec_time_int"] = int(datetime.strptime(item["msg_time"], DATE_FORMAT).timestamp())
                    item_dict["read_time_int"] = int(datetime.strptime(item["msg_time"], DATE_FORMAT).timestamp())
                    lp_data.append(item_dict)

                for item in lpi_result:
                    item_dict = dict(zip(lpi_keys, item))
                    item_dict["msg_time_int"] = int(datetime.strptime(item["msg_time"], DATE_FORMAT).timestamp())
                    item_dict["rec_time_int"] = int(datetime.strptime(item["msg_time"], DATE_FORMAT).timestamp())
                    item_dict["read_time_int"] = int(datetime.strptime(item["msg_time"], DATE_FORMAT).timestamp())
                    lpi_data.append(item_dict)

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
