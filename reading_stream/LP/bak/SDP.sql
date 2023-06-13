Select
  sdp_id,
  cust_id,
  meter_id,
  EXTRACT(
    EPOCH
    FROM
      (sdp_start_time :: timestamp)
  ) as begin,
  EXTRACT(
    EPOCH
    FROM
      (
        COALESCE(sdp_end_time, '9999-12-31') :: timestamp
      )
  ) as end,
sdp_start_time,
sdp_end_time,
ctpt_ratio,
ct_ratio,
pt_ratio,
pwr_co_id,
tou_id,
aprv_eng
from
  sdp
order by
  sdp_id,
  sdp_start_time
