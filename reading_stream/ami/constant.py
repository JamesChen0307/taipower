#!/usr/bin/ python3
"""
 Code Desctiption：

 LP 讀表串流處理作業-共用 constant 代碼資訊
  (1) 讀表欄項對應代碼
  (2) ReadingQualityCode對應代碼
"""
# Author：babylon@cht.com.tw
# Date：2023/04/29
#
# Modified by：
#

from ami import func

"""
 Load Profile P6 XML ReadingType 對應欄位資訊
"""
LOADPROFILE = {
    # 臨時
    # "0.0.0.0.0.2.167.0.0.0.0.0.0.0.0.0.0.0": {
    #     "name": "TOU_ID",
    #     "type": "varchar",
    # },  # 程式ID號碼 (TOU_id) 處理台達程式
    # 正式代表一個正式的資料，它的名稱為 "DEL_KWH"，型別為 "double"
    "0.0.2.9.1.2.12.0.0.0.0.0.0.0.0.3.72.0": {
        "name": "DEL_KWH",
        "type": "double",
    },  # 售電總仟瓦小時(Del total kWh)
    "0.0.2.9.19.2.12.0.0.0.0.0.0.0.0.3.72.0": {
        "name": "REC_KWH",
        "type": "double",
    },  # 購電總仟瓦小時 (Rec total kWh)
    "0.0.2.9.15.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "DEL_KVARH_LAG",
        "type": "double",
    },  # 總售電kVArh (Q1) (Total Del kVArh delivered lagging)
    "0.0.2.9.16.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "DEL_KVARH_LEAD",
        "type": "double",
    },  # 總售電kVArh (Q2) (Total Del kVArh delivered leading)
    "0.0.2.9.17.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "REC_KVARH_LAG",
        "type": "double",
    },  # 總購電kVArh (Q3) (Total rec kVArh received lagging), default=0
    "0.0.2.9.18.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "REC_KVARH_LEAD",
        "type": "double",
    },  # 總購電kVArh (Q4) (Total rec kVArh received leading), default=0
    "11.0.0.0.0.2.119.0.0.0.0.0.0.0.0.0.159.0": {
        "name": "BATTERY_TIME",
        "type": "bigint",
    },  # 電池使用時間(Battery carryover time)(分)
    "11.0.0.0.0.2.167.0.0.0.0.0.0.0.0.0.0.0": {
        "name": "TOU_ID",
        "type": "varchar",
    },  # 程式ID號碼 (TOU_id)
    "11.22.103.0.0.2.122.0.0.0.0.0.0.0.0.0.108.0": {
        "name": "RESET_TIME",
        "type": "timestamp",
    },  # 最近一次需量復歸日期時間 (Date of last reset)(MN, DR: 只有日期, 時間補0, ARL: 日期+時間)
    "11.8.0.0.1.2.8.0.0.0.0.0.0.0.0.0.108.0": {
        "name": "DEL_MAX_KW_TIME",
        "type": "timestamp",
    },  # 售電最大需量之日期時間 (Del date of maximum kW)
    "0.0.0.0.0.2.166.0.0.0.0.0.0.0.0.0.0.0": {
        "name": "CTPT_RATIO",
        "type": "bigint",
    },  # 變比器倍數(Transformer factor)
    "0.0.0.0.0.2.168.0.0.0.0.0.0.0.0.0.0.0": {
        "name": "CT_RATIO",
        "type": "integer",
    },  # CT 比
    "0.0.0.0.0.2.169.0.0.0.0.0.0.0.0.0.0.0": {
        "name": "PT_RATIO",
        "type": "integer",
    },  # PT 比
    "11.0.0.9.1.2.12.0.0.0.0.0.0.0.0.3.72.0": {
        "name": "DEL_KWH",
        "type": "double",
    },  # 售電總仟瓦小時(Del total kWh)
    "11.0.0.9.1.2.12.0.0.0.0.1.0.0.0.3.72.0": {
        "name": "DEL_RATE_A_KWH",
        "type": "double",
    },  # 售電尖峰仟瓦小時 (Del rate A total kWh)
    "11.0.0.6.1.2.8.0.0.0.0.1.0.0.0.3.38.0": {
        "name": "DEL_RATE_A_MAX_KW",
        "type": "double",
    },  # 售電尖峰需量(指示) (Del rate A maximum kW)
    "11.0.0.3.1.2.8.0.0.0.0.1.0.0.0.3.38.0": {
        "name": "DEL_RATE_A_CUM_KW",
        "type": "double",
    },  # 售電尖峰需量(累計) (Del rate A cum. kW)
    "11.0.0.2.1.2.8.0.0.0.0.1.0.0.0.3.38.0": {
        "name": "DEL_RATE_A_CONT_KW",
        "type": "double",
    },  # 售電尖峰需量(連續累計) (Del rate A cont. cum. kW)
    "11.0.0.9.1.2.12.0.0.0.0.2.0.0.0.3.72.0": {
        "name": "DEL_RATE_B_KWH",
        "type": "double",
    },  # 售電平日半尖峰仟瓦小時 (Del rate B total kWh)
    "11.0.0.6.1.2.8.0.0.0.0.2.0.0.0.3.38.0": {
        "name": "DEL_RATE_B_MAX_KW",
        "type": "double",
    },  # 售電平日半尖峰需量(指示) (Del rate B maximum kW)
    "11.0.0.3.1.2.8.0.0.0.0.2.0.0.0.3.38.0": {
        "name": "DEL_RATE_B_CUM_KW",
        "type": "double",
    },  # 售電平日半尖峰需量(累計) (Del rate B cum. kW)
    "11.0.0.2.1.2.8.0.0.0.0.2.0.0.0.3.38.0": {
        "name": "DEL_RATE_B_CONT_KW",
        "type": "double",
    },  # 售電平日半尖峰需量(連續累計) (Del rate B cont. cum. kW)
    "11.0.0.9.1.2.12.0.0.0.0.3.0.0.0.3.72.0": {
        "name": "DEL_RATE_C_KWH",
        "type": "double",
    },  # 售電離峰仟瓦小時 (Del rate C total kWh)
    "11.0.0.6.1.2.8.0.0.0.0.3.0.0.0.3.38.0": {
        "name": "DEL_RATE_C_MAX_KW",
        "type": "double",
    },  # 售電離峰需量(指示) (Del rate C maximum kW)
    "11.0.0.3.1.2.8.0.0.0.0.3.0.0.0.3.38.0": {
        "name": "DEL_RATE_C_CUM_KW",
        "type": "double",
    },  # 售電離峰需量(累計) (Del rate C cum. kW)
    "11.0.0.2.1.2.8.0.0.0.0.3.0.0.0.3.38.0": {
        "name": "DEL_RATE_C_CONT_KW",
        "type": "double",
    },  # 售電離峰需量(連續累計) (Del rate C cont. cum. kW)
    "11.0.0.9.1.2.12.0.0.0.0.4.0.0.0.3.72.0": {
        "name": "DEL_RATE_D_KWH",
        "type": "double",
    },  # 售電週六半尖峰仟瓦小時 (Del rate D total kWh)
    "11.0.0.6.1.2.8.0.0.0.0.4.0.0.0.3.38.0": {
        "name": "DEL_RATE_D_MAX_KW",
        "type": "double",
    },  # 售電週六半尖峰需量(指示) (Del rate D maximum kW)
    "11.0.0.3.1.2.8.0.0.0.0.4.0.0.0.3.38.0": {
        "name": "DEL_RATE_D_CUM_KW",
        "type": "double",
    },  # 售電週六半尖峰需量(累計) (Del rate D cum. kW)
    "11.0.0.2.1.2.8.0.0.0.0.4.0.0.0.3.38.0": {
        "name": "DEL_RATE_D_CONT_KW",
        "type": "double",
    },  # 售電週六半尖峰需量(連續累計) (Del rate D cont. cum. kW)
    "11.0.0.0.0.2.122.0.0.0.0.0.0.0.0.0.111.0": {
        "name": "RESET_COUNT",
        "type": "integer",
    },  # 需量復歸次數 (Numbers of demand reset)
    "11.0.2.9.15.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "DEL_LAG_KVARH",
        "type": "double",
    },  # 總售電kVArh (Q1) (Total Del kVArh delivered lagging)
    "11.0.0.9.16.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "DEL_LEAD_KVARH",
        "type": "double",
    },  # 總售電kVArh (Q2) (Total Del kVArh delivered leading)
    "11.0.0.9.17.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "REC_LAG_KVARH",
        "type": "double",
    },  # 總購電kVArh (Q3) (Total rec kVArh received lagging)
    "11.0.2.9.18.2.164.0.0.0.0.0.0.0.0.3.73.0": {
        "name": "REC_LEAD_KVARH",
        "type": "double",
    },  # 總購電kVArh (Q4) (Total rec kVArh received leading)
    "0.22.103.0.0.2.139.0.0.0.0.0.0.0.0.0.108.0": {
        "name": "PROG_DATE",
        "type": "timestamp",
    },  # 最後程式日期 (Datetime last programmed)
    "0.4.0.0.0.2.4.0.0.0.0.0.0.0.16.0.109.0": {
        "name": "PHASE_N_CURR_OVER",
        "type": "boolean",
    },  # 中性線電流過高 (Phase N over current)
    "11.8.0.0.19.2.8.0.0.0.0.0.0.0.0.0.108.0": {
        "name": "REC_MAX_KW_TIME",
        "type": "timestamp",
    },  # 購電最大需量日期時間 (Rec date of maximum kW)
    "11.0.0.9.19.2.12.0.0.0.0.0.0.0.0.3.72.0": {
        "name": "REC_KWH",
        "type": "double",
    },  # 購電總仟瓦小時 (Rec total kWh)
    "11.0.0.9.19.2.12.0.0.0.0.1.0.0.0.3.72.0": {
        "name": "REC_RATE_A_KWH",
        "type": "double",
    },  # 購電尖峰仟瓦小時 (Rec rate A total kWh)
    "11.0.0.6.19.2.8.0.0.0.0.1.0.0.0.3.38.0": {
        "name": "REC_RATE_A_MAX_KW",
        "type": "double",
    },  # 購電尖峰需量(指示) (Rec rate A maximum kW)
    "11.0.0.3.19.2.8.0.0.0.0.1.0.0.0.3.38.0": {
        "name": "REC_RATE_A_CUM_KW",
        "type": "double",
    },  # 購電尖峰需量(累計) (Rec rate A cum. kW)
    "11.0.0.2.19.2.8.0.0.0.0.1.0.0.0.3.38.0": {
        "name": "REC_RATE_A_CONT_KW",
        "type": "double",
    },  # 購電尖峰需量(連續累計) (Rec rate A cont. cum. kW)
    "11.0.0.9.19.2.12.0.0.0.0.2.0.0.0.3.72.0": {
        "name": "REC_RATE_B_KWH",
        "type": "double",
    },  # 購電平日半尖峰仟瓦小時 (Rec rate B total kWh)
    "11.0.0.6.19.2.8.0.0.0.0.2.0.0.0.3.38.0": {
        "name": "REC_RATE_B_MAX_KW",
        "type": "double",
    },  # 購電平日半尖峰需量(指示) (Rec rate B maximum kW)
    "11.0.0.3.19.2.8.0.0.0.0.2.0.0.0.3.38.0": {
        "name": "REC_RATE_B_CUM_KW",
        "type": "double",
    },  # 購電平日半尖峰需量(累計) (Rec rate B cum. kW)
    "11.0.0.2.19.2.8.0.0.0.0.2.0.0.0.3.38.0": {
        "name": "REC_RATE_B_CONT_KW",
        "type": "double",
    },  # 購電平日半尖峰需量(連續累計) (Rec rate B cont. cum. kW)
    "11.0.0.9.19.2.12.0.0.0.0.3.0.0.0.3.72.0": {
        "name": "REC_RATE_C_KWH",
        "type": "double",
    },  # 購電離峰仟瓦小時 (Rec rate C total kWh)
    "11.0.0.6.19.2.8.0.0.0.0.3.0.0.0.3.38.0": {
        "name": "REC_RATE_C_MAX_KW",
        "type": "double",
    },  # 購電離峰需量(指示) (Rec rate C maximum kW)
    "11.0.0.3.19.2.8.0.0.0.0.3.0.0.0.3.38.0": {
        "name": "REC_RATE_C_CUM_KW",
        "type": "double",
    },  # 購電離峰需量(累計) (Rec rate C cum. kW)
    "11.0.0.2.19.2.8.0.0.0.0.3.0.0.0.3.38.0": {
        "name": "REC_RATE_C_CONT_KW",
        "type": "double",
    },  # 購電離峰需量(連續累計) (Rec rate C cont. cum. kW)
    "11.0.0.9.19.2.12.0.0.0.0.4.0.0.0.3.72.0": {
        "name": "REC_RATE_D_KWH",
        "type": "double",
    },  # 購電週六半尖峰仟瓦小時 (Rec rate D total kWh)
    "11.0.0.6.19.2.8.0.0.0.0.4.0.0.0.3.38.0": {
        "name": "REC_RATE_D_MAX_KW",
        "type": "double",
    },  # 購電週六半尖峰需量(指示) (Rec rate D maximum kW)
    "11.0.0.3.19.2.8.0.0.0.0.4.0.0.0.3.38.0": {
        "name": "REC_RATE_D_CUM_KW",
        "type": "double",
    },  # 購電週六半尖峰需量(累計) (Rec rate D cum. kW)
    "11.0.0.2.19.2.8.0.0.0.0.4.0.0.0.3.38.0": {
        "name": "REC_RATE_D_CONT_KW",
        "type": "double",
    },  # 購電週六半尖峰需量(連續累計) (Rec rate D cont. cum. kW)
    "11.8.0.0.1.2.8.0.0.0.0.1.0.0.0.0.108.0": {
        "name": "DEL_RATE_A_MAX_KW_TIME",
        "type": "timestamp",
    },  # 售電尖峰最大需量之日期時間 (Del rate A date of maximum kW)
    "11.8.0.0.1.2.8.0.0.0.0.2.0.0.0.0.108.0": {
        "name": "DEL_RATE_B_MAX_KW_TIME",
        "type": "timestamp",
    },  # 售電平日半尖峰最大需量之日期時間 (Del rate B date of maximum kW)
    "11.8.0.0.1.2.8.0.0.0.0.3.0.0.0.0.108.0": {
        "name": "DEL_RATE_C_MAX_KW_TIME",
        "type": "timestamp",
    },  # 售電離峰最大需量之日期時間 (Del rate C date of maximum kW)
    "11.8.0.0.1.2.8.0.0.0.0.4.0.0.0.0.108.0": {
        "name": "DEL_RATE_D_MAX_KW_TIME",
        "type": "timestamp",
    },  # 售電週六半尖峰最大需量之日期時間 (Del rate D date of maximum kW)
    "0.0.0.12.19.2.8.0.0.0.0.0.0.0.0.3.38.0": {
        "name": "REC_INST_KW",
        "type": "double",
    },  # 購電瞬時需量kW
    "11.0.0.9.19.2.12.0.0.0.0.5.0.0.0.3.72.0": {
        "name": "REC_RATE_E_KWH",
        "type": "double",
    },  # 購電E峰仟瓦小時 (Rec rate E total kWh)
    "11.0.0.6.19.2.8.0.0.0.0.5.0.0.0.3.38.0": {
        "name": "REC_RATE_E_MAX_KW",
        "type": "double",
    },  # 購電E峰需量(指示) (Rec rate E maximum kW)
    "11.0.0.3.19.2.8.0.0.0.0.5.0.0.0.3.38.0": {
        "name": "REC_RATE_E_CUM_KW",
        "type": "double",
    },  # 購電E峰需量(累計) (Rec rate E cum. kW)
    "11.0.0.2.19.2.8.0.0.0.0.5.0.0.0.3.38.0": {
        "name": "REC_RATE_E_CONT_KW",
        "type": "double",
    },  # 購電E峰需量(連續累計) (Rec rate E cont. cum. kW)
    "11.0.0.9.19.2.12.0.0.0.0.6.0.0.0.3.72.0": {
        "name": "REC_RATE_F_KWH",
        "type": "double",
    },  # 購電F峰仟瓦小時 (Rec rate F total kWh)
    "11.0.0.6.19.2.8.0.0.0.0.6.0.0.0.3.38.0": {
        "name": "REC_RATE_F_MAX_KW",
        "type": "double",
    },  # 購電F峰需量(指示) (Rec rate F maximum kW)
    "11.0.0.3.19.2.8.0.0.0.0.6.0.0.0.3.38.0": {
        "name": "REC_RATE_F_CUM_KW",
        "type": "double",
    },  # 購電F峰需量(累計) (Rec rate F cum. kW)
    "11.0.0.2.19.2.8.0.0.0.0.6.0.0.0.3.38.0": {
        "name": "REC_RATE_F_CONT_KW",
        "type": "double",
    },  # 購電F峰需量(連續累計) (Rec rate F cont. cum. kW)
    "11.0.0.9.1.2.12.0.0.0.0.5.0.0.0.3.72.0": {
        "name": "DEL_RATE_E_KWH",
        "type": "double",
    },  # 售電E峰仟瓦小時 (Del rate D total kWh)
    "11.0.0.6.1.2.8.0.0.0.0.5.0.0.0.3.38.0": {
        "name": "DEL_RATE_E_MAX_KW",
        "type": "double",
    },  # 售電E峰需量(指示) (Del rate E maximum kW)
    "11.0.0.3.1.2.8.0.0.0.0.5.0.0.0.3.38.0": {
        "name": "DEL_RATE_E_CUM_KW",
        "type": "double",
    },  # 售電E峰需量(累計) (Del rate E cum. kW)
    "11.0.0.2.1.2.8.0.0.0.0.5.0.0.0.3.38.0": {
        "name": "DEL_RATE_E_CONT_KW",
        "type": "double",
    },  # 售電E峰需量(連續累計) (Del rate E cont. cum. kW)
    "11.0.0.9.1.2.12.0.0.0.0.6.0.0.0.3.72.0": {
        "name": "DEL_RATE_F_KWH",
        "type": "double",
    },  # 售電F峰仟瓦小時 (Del rate F total kWh)
    "11.0.0.6.1.2.8.0.0.0.0.6.0.0.0.3.38.0": {
        "name": "DEL_RATE_F_MAX_KW",
        "type": "double",
    },  # 售電F峰需量(指示) (Del rate F maximum kW)
    "11.0.0.3.1.2.8.0.0.0.0.6.0.0.0.3.38.0": {
        "name": "DEL_RATE_F_CUM_KW",
        "type": "double",
    },  # 售電F峰需量(累計) (Del rate F cum. kW)
    "11.0.0.2.1.2.8.0.0.0.0.6.0.0.0.3.38.0": {
        "name": "DEL_RATE_F_CONT_KW",
        "type": "double",
    },  # 售電F峰需量(連續累計) (Del rate F cont. cum. kW)
    "11.8.0.0.1.2.8.0.0.0.0.5.0.0.0.0.108.0": {
        "name": "DEL_RATE_E_MAX_KW_TIME",
        "type": "timestamp",
    },  # 售電E峰最大需量之日期時間 (Del rate E date of maximum kW)
    "11.8.0.0.1.2.8.0.0.0.0.6.0.0.0.0.108.0": {
        "name": "DEL_RATE_F_MAX_KW_TIME",
        "type": "timestamp",
    },  # 售電F峰最大需量之日期時間 (Del rate F date of maximum kW)
    "11.0.104.0.0.2.8.0.0.0.0.0.0.0.0.0.27.0": {
        "name": "DEMAND_REMAIN_SEC",
        "type": "integer",
    },  # 需量時段剩餘時間 (Demand_interval_remain_sec)
    "11.0.0.12.0.2.12.0.0.0.0.0.0.0.0.3.61.0": {
        "name": "INST_KVA",
        "type": "double",
    },  # 瞬時kVA (Instantaneous kVA)
    "11.0.0.12.0.2.8.0.0.0.0.0.0.0.0.3.38.0": {
        "name": "INST_KW",
        "type": "double",
    },  # 瞬時需量 (Instantaneous kW)
    "11.0.0.12.0.2.38.0.0.0.0.0.0.0.0.0.65.0": {
        "name": "INT_PF",
        "type": "float",
    },  # 瞬時功率因數 (Instantaneous PF)
    "0.0.0.0.0.2.902.0.0.0.0.0.0.0.0.0.0.0": {
        "name": "ID2",
        "type": "varchar",
    },  # 區處代號ID2
    "0.0.0.0.0.2.901.0.0.0.0.0.0.0.0.0.0.0": {
        "name": "ID1",
        "type": "varchar",
    },  # 電號ID1
    "0.0.79.6.0.2.54.0.0.0.0.0.0.0.128.0.29.0": {
        "name": "PHASE_A_VOLT",
        "type": "float",
    },  # A 相電壓 (Phase A voltage)
    "0.0.79.6.0.2.54.0.0.0.0.0.0.0.64.0.29.0": {
        "name": "PHASE_B_VOLT",
        "type": "float",
    },  # B 相電壓 (Phase B voltage)
    "0.0.79.6.0.2.54.0.0.0.0.0.0.0.32.0.29.0": {
        "name": "PHASE_C_VOLT",
        "type": "float",
    },  # C 相電壓 (Phase C voltage)
    "0.0.79.6.0.2.4.0.0.0.0.0.0.0.128.0.5.0": {
        "name": "PHASE_A_CURR",
        "type": "float",
    },  # A 相電流 (Phase A current)
    "0.0.79.6.0.2.4.0.0.0.0.0.0.0.64.0.5.0": {
        "name": "PHASE_B_CURR",
        "type": "float",
    },  # B 相電流 (Phase B current)
    "0.0.79.6.0.2.4.0.0.0.0.0.0.0.32.0.5.0": {
        "name": "PHASE_C_CURR",
        "type": "float",
    },  # C 相電流 (Phase C current)
    "0.0.79.6.0.2.4.0.0.0.0.0.0.0.16.0.5.0": {
        "name": "PHASE_N_CURR",
        "type": "float",
    },  # N 相電流 (Phase N current)
    "0.0.79.6.0.2.55.0.0.0.0.0.0.0.128.0.9.0": {
        "name": "PHASE_A_VOLT_ANGLE",
        "type": "float",
    },  # A 相電壓相角 (Phase A voltage degree)
    "0.0.79.6.0.2.55.0.0.0.0.0.0.0.64.0.9.0": {
        "name": "PHASE_B_VOLT_ANGLE",
        "type": "float",
    },  # B 相電壓相角 (Phase B voltage degree)
    "0.0.79.6.0.2.55.0.0.0.0.0.0.0.32.0.9.0": {
        "name": "PHASE_C_VOLT_ANGLE",
        "type": "float",
    },  # C 相電壓相角 (Phase C voltage degree)
    "0.0.79.6.0.2.5.0.0.0.0.0.0.0.128.0.9.0": {
        "name": "PHASE_A_CURR_ANGLE",
        "type": "float",
    },  # A 相電流相角 (Phase A current degree)
    "0.0.79.6.0.2.5.0.0.0.0.0.0.0.64.0.9.0": {
        "name": "PHASE_B_CURR_ANGLE",
        "type": "float",
    },  # B 相電流相角 (Phase B current degree)
    "0.0.79.6.0.2.5.0.0.0.0.0.0.0.32.0.9.0": {
        "name": "PHASE_C_CURR_ANGLE",
        "type": "float",
    },  # C 相電流相角 (Phase C current degree)
    "0.2.79.0.1.2.38.0.0.0.0.0.0.0.0.0.65.0": {
        "name": "DEL_AVG_PF",
        "type": "float",
    },  # 售電平均功率因數 (Del AVG. power factor)
    "0.0.79.6.0.2.54.0.0.0.0.0.0.0.132.0.29.0": {
        "name": "LINE_AB_VOLT",
        "type": "float",
    },  # AB 線電壓值 (AB_line_vol) 1.0.32.7.128.255
    "0.0.79.6.0.2.4.0.0.0.0.0.0.0.132.0.5.0": {
        "name": "LINE_AB_CURR",
        "type": "float",
    },  # AB 線電流值 (AB_line_cur) 1.0.31.7.128 .255
    "0.0.79.6.0.2.55.0.0.0.0.0.0.0.132.0.9.0": {
        "name": "LINE_AB_VOLT_ANGLE",
        "type": "float",
    },  # AB 線電壓角度 (AB_line_vol_ang) 1.0.81.7.10.255
    "0.0.0.6.0.2.5.0.0.0.0.0.0.0.132.0.9.0": {
        "name": "LINE_AB_CURR_ANGLE",
        "type": "float",
    },  # AB 線電流角度 (AB_line_cur_ang)
    "0.2.79.0.19.2.38.0.0.0.0.0.0.0.0.0.65.0": {
        "name": "REC_AVG_PF",
        "type": "float",
    },  # 購電平均功率因數 (Rec AVG. power factor)
    "0.0.0.0.0.2.15.0.0.0.0.0.0.0.0.0.33.0": {"name": "HZ", "type": "float"},  # Hz頻率
    "0.0.0.0.0.2.38.0.0.0.0.0.0.0.128.0.65.0": {
        "name": "PHASE_A_PF",
        "type": "float",
    },  # A 相功率因素
    "0.0.0.0.0.2.38.0.0.0.0.0.0.0.64.0.65.0": {
        "name": "PHASE_B_PF",
        "type": "float",
    },  # B 相功率因素
    "0.0.0.0.0.2.38.0.0.0.0.0.0.0.32.0.65.0": {
        "name": "PHASE_C_PF",
        "type": "float",
    },  # C 相功率因素
}

"""
 Load Profile P6 XML ReadingType 對應欄位資訊
"""
QUALITYCODE = {
    "1.0.0": {"interval": 1, "note": 1},
    "1.4.2001": {"interval": 2, "note": 2},  # Partial/Short Interval + 校時原因
    "1.4.2002": {"interval": 2, "note": 3},  # Partial/Short Interval + 斷復電原因
    "1.4.3001": {"interval": 3, "note": 2},  # Long Interval + 校時原因
    "1.4.3002": {"interval": 3, "note": 3},  # Long Interval + 斷復電原因
    "1.4.4": {"interval": 1, "note": 3},  # 讀錶值未產生
    # "1.5.257": {"interval": 4, "note": 1}, # 無效資料
    "3.7.0": {"interval": 6, "note": 1},  # 人工輸入 + 一般原因
    "3.7.1": {"interval": 6, "note": 5},  # 人工輸入 + 電表故障原因
    "3.7.2": {"interval": 6, "note": 7},  # 人工輸入 + 換表原因
}




csv_header = [
    "FAN_ID",
    "SOURCE",
    "CUST_ID",
    "METER_ID",
    "READ_TIME",
    "REC_NO",
    "DEL_KWH",
    "REC_KWH",
    "DEL_KVARH_LAG",
    "DEL_KVARH_LEAD",
    "REC_KVARH_LAG",
    "REC_KVARH_LEAD",
    "INTERVAL",
    "NOTE",
]
