from datetime import datetime
from decimal import Decimal
from typing import Optional, Union

from pydantic import (BaseModel, Field, NegativeInt, PositiveInt,
                      ValidationError, condecimal, conint, conlist, constr,
                      root_validator, validator)

"""
 Code Desctiption：

 LP 讀表串流處理作業
  (1) LP 讀表欄位格式檢核
  (2)
"""
# Author：OOO
# Date：OOOO/OO/OO
#
# Modified by：[V0.03][20230507][babylon][補上格式修正constant.warn_func](sample)
# Modified by：[V0.02][20230502][babylon][新增validation作業](sample)
#


"""
 LP 讀表 Class Obj. 定義
"""


class LpRawTemp(BaseModel):
    source: str
    meter_id: constr(max_length=10)
    fan_id: constr(max_length=32)
    rec_no: Optional[int]
    read_time: datetime
    interval: int
    note: int
    del_kwh: float
    rec_kwh: Optional[float]
    del_kvarh_lag: Optional[float]
    del_kvarh_lead: Optional[float]
    rec_kvarh_lag: Optional[float]
    rec_kvarh_lead: Optional[float]
    sdp_id: Optional[str]
    ratio: Optional[str]
    pwr_co_id: Optional[str]
    cust_id: Optional[str]
    ct_ratio: Optional[str]
    pt_ratio: Optional[str]
    file_type: constr(max_length=6)
    raw_gzfile: Optional[constr(max_length=200)]
    raw_file: Optional[constr(max_length=200)]
    rec_time: Optional[Union[datetime, str]]
    file_path: Optional[constr(max_length=200)]
    file_size: int
    file_seqno: int
    msg_id: Optional[constr(max_length=36)]
    corr_id: Optional[constr(max_length=36)]
    msg_time: Optional[Union[datetime, str]]
    read_group: Optional[str]
    verb: Optional[str]
    noun: Optional[str]
    context: Optional[str]
    msg_idx: Optional[str]
    rev: int
    qos: constr(max_length=7)
    start_strm_time: Optional[datetime]
    warn_dur_ts: Optional[str]
    main_dur_ts: Optional[str]
    rt_count: int

    # ---------------------------------------------------------------------------- #
    #                                   Datetime                                   #
    # ---------------------------------------------------------------------------- #

    # --------------------------------- read_time -------------------------------- #
    @validator("read_time")
    def dt_colum_check(cls, v, values):
        # 未來日期
        sys_time = datetime.now()
        if v > sys_time:
            raise ValueError("W21001")

        # 不檢查特定日期之通訊商讀表資料
        start_date = datetime(2023, 1, 19)
        end_date = datetime(2023, 1, 21)
        if (
            "source" in values
            and values["source"] == "HES-DAS20180705"
            and start_date <= v <= end_date
        ):
            raise ValueError("W24001")

        # 讀表read_time大於檔案rec_time
        if "rec_time" in values and values["rec_time"] < v:
            raise ValueError("W21002")

    # ---------------------------------------------------------------------------- #
    #                                    Double                                    #
    # ---------------------------------------------------------------------------- #

    # ------------------ del_kwh, del_kvarh_lag, del_kvarh_lead ------------------ #
    # ------------------ rec_kwh, rec_kvarh_lag, rec_kvarh_lead ------------------ #
    @validator(
        "del_kwh", "rec_kwh", "del_kvarh_lag", "del_kvarh_lead", "rec_kvarh_lag", "rec_kvarh_lead"
    )
    def dou_colum_check(cls, v):
        # 大於等於資料庫上限-整數14位小數4位
        if v is None:
            return v
        database_limit = 99999999999999.9999
        if v >= database_limit:
            raise ValueError("W20005")

        # 小於0
        if v < 0:
            raise ValueError("W20005")

        # 低壓用戶與高壓用戶讀值超位修正
        int_part = int(v)
        int_len = len(str(int_part))
        dec_len = len(str(v)[int_len + 1 :])

        if int_len > 5 or dec_len > 4:
            raise ValueError("W21010")

    # ---------------------------------------------------------------------------- #
    #                                    Integer                                   #
    # ---------------------------------------------------------------------------- #

    # --------------------------------- rt_count --------------------------------- #
    @validator("rt_count")
    def rt_count_zero(cls, v):
        # 該筆資料無任何讀值項目(rt_count=0)
        if v == 0:
            raise ValueError("W23001")

    # --------------------- rec_no, interval, note, file_size -------------------- #
    # ------------------------- file_seqno, rev, rt_count ------------------------ #
    @validator(
        "rec_no",
        "interval",
        "note",
        "file_size",
        "file_seqno",
        "rev",
        "rt_count",
    )
    def int_colum_check(cls, v):
        if isinstance(v, (int)):
            # 小於0
            if v < 0:
                raise ValueError("W20006")

            # 大於等於資料庫上限
            if v > 2147483647:
                raise ValueError("W20006")




class LpiRawTemp(BaseModel):
    source: str
    meter_id: constr(max_length=10)
    fan_id: constr(max_length=32)
    tamp_cust_id: constr(max_length=11)
    comment: Optional[Union[int, str]]
    read_time: datetime
    interval: int
    note: int
    del_kwh: float
    rec_kwh: Optional[float]
    del_kvarh_lag: Optional[float]
    del_kvarh_lead: Optional[float]
    rec_kvarh_lag: Optional[float]
    rec_kvarh_lead: Optional[float]
    sdp_id: Optional[str]
    ratio: Optional[str]
    pwr_co_id: Optional[str]
    cust_id: Optional[str]
    ct_ratio: Optional[str]
    pt_ratio: Optional[str]
    file_type: constr(max_length=6)
    raw_gzfile: Optional[constr(max_length=200)]
    raw_file: Optional[constr(max_length=200)]
    rec_time: Optional[Union[datetime, str]]
    file_path: Optional[constr(max_length=200)]
    file_size: int
    file_seqno: int
    msg_id: Optional[constr(max_length=36)]
    corr_id: Optional[constr(max_length=36)]
    msg_time: Optional[Union[datetime, str]]
    read_group: Optional[str]
    verb: Optional[str]
    noun: Optional[str]
    context: Optional[str]
    msg_idx: Optional[str]
    rev: Optional[Union[int, str]]
    qos: constr(max_length=7)
    start_strm_time: Optional[datetime]
    warn_dur_ts: Optional[str]
    main_dur_ts: Optional[str]
    rt_count: int

    # ---------------------------------------------------------------------------- #
    #                                   Datetime                                   #
    # ---------------------------------------------------------------------------- #

    # --------------------------------- read_time -------------------------------- #
    @validator("read_time")
    def dt_colum_check(cls, v, values):
        # 未來日期
        sys_time = datetime.now()
        if v > sys_time:
            raise ValueError("W21001")

        # 不檢查特定日期之通訊商讀表資料
        start_date = datetime(2023, 1, 19)
        end_date = datetime(2023, 1, 21)
        if (
            "source" in values
            and values["source"] == "HES-DAS20180705"
            and start_date <= v <= end_date
        ):
            raise ValueError("W24001")

        # 讀表read_time大於檔案rec_time
        if "rec_time" in values and values["rec_time"] < v:
            raise ValueError("W21002")

    # ---------------------------------------------------------------------------- #
    #                                    Double                                    #
    # ---------------------------------------------------------------------------- #

    # ------------------ del_kwh, del_kvarh_lag, del_kvarh_lead ------------------ #
    # ------------------ rec_kwh, rec_kvarh_lag, rec_kvarh_lead ------------------ #
    @validator(
        "del_kwh", "rec_kwh", "del_kvarh_lag", "del_kvarh_lead", "rec_kvarh_lag", "rec_kvarh_lead"
    )
    def dou_colum_check(cls, v):
        # 大於等於資料庫上限-整數14位小數4位
        if v is None:
            return v
        database_limit = 99999999999999.9999
        if v >= database_limit:
            raise ValueError("W20005")

        # 小於0
        if v < 0:
            raise ValueError("W20005")

        # 低壓用戶與高壓用戶讀值超位修正
        int_part = int(v)
        int_len = len(str(int_part))
        dec_len = len(str(v)[int_len + 1 :])

        if int_len > 5 or dec_len > 4:
            raise ValueError("W21010")

    # ---------------------------------------------------------------------------- #
    #                                    Integer                                   #
    # ---------------------------------------------------------------------------- #

    # --------------------- comment, interval, note, file_size -------------------- #
    # ------------------------- file_seqno, rev, rt_count ------------------------ #
    @validator(
        "comment",
        "interval",
        "note",
        "file_size",
        "file_seqno",
        "rev",
        "rt_count",
    )
    def int_colum_check(cls, v):
        if isinstance(v, (int)):
            # 小於0
            if v < 0:
                raise ValueError("W20006")

            # 大於等於資料庫上限
            if v > 2147483647:
                raise ValueError("W20006")

    # --------------------------------- rt_count --------------------------------- #
    @validator("rt_count")
    def rt_count_zero(cls, v):
        # 該筆資料無任何讀值項目(rt_count=0)
        if v == 0:
            raise ValueError("W23001")


class FileLog(BaseModel):
    log_date_int: Optional[int]
    file_type: Optional[str]
    raw_gzfile: Optional[str]
    raw_file: Optional[str]
    rec_time: Optional[str]
    file_path: Optional[str]
    file_dir_ym: Optional[str]
    file_dir_date: Optional[str]
    file_seqno: Optional[int]
    msg_id: Optional[str]
    source: Optional[str]
    read_group: Optional[str]
    total_cnt: Optional[int]
    warn_cnt: Optional[int]
    main_succ_cnt: Optional[int]
    dedup_cnt: Optional[int]
    err_cnt: Optional[int]
    dup_cnt: Optional[int]
    hist_cnt: Optional[int]
    wait_cnt: Optional[int]
    fnsh_cnt: Optional[int]
    proc_type: Optional[int]
    file_batch_no: Optional[str]
    batch_mk: Optional[str]
    log_start_time: Optional[str]
    log_upd_time: Optional[str]
    log_end_time: Optional[str]
    dw_update_time: Optional[str]


class WarnLog(BaseModel):
    file_type: Optional[str]
    raw_gzfile: Optional[str]
    raw_file: Optional[str]
    rec_time: Optional[str]
    file_path: Optional[str]
    file_seqno: Optional[int]
    source: Optional[str]
    read_group: Optional[str]
    meter_id: Optional[str]
    read_time: Optional[str]
    type_cd: Optional[str]
    col_nm: Optional[str]
    rt_count: Optional[int]
    log_data_time: Optional[str]


class ErrorLog(BaseModel):
    file_type: Optional[str]
    raw_gzfile: Optional[str]
    raw_file: Optional[str]
    rec_time: Optional[str]
    file_path: Optional[str]
    file_seqno: Optional[int]
    source: Optional[str]
    read_group: Optional[str]
    meter_id: Optional[str]
    read_time: Optional[str]
    type_cd: Optional[str]
    log_data_time: Optional[str]

