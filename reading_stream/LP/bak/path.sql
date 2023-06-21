select
  file_batch_no,
  bucket_nm,
  read_group,
  to_char(date_trunc('month', dt), 'yyyy-mm-dd') as file_dir_ym,
  to_char(dt, 'yyyy-mm-dd') as file_dir_date,
  case
    when bucket_nm is not null then bucket_nm || '/' || lower(read_group) || to_char(dt, '/yyyy/mm/dd/')
    else path_nm
  end as file_path,
  0 as file_cnt,
  0 as proc_cnt,
  batch_mk,
  0 as proc_type,
  TO_CHAR(now(), 'YYYY-MM-DD HH24:MI:SS') as crtd_time,
  null as log_start_time,
  null as log_upd_time,
  null as log_end_time
from
  (
    select
      file_batch_no,
      bucket_nm,
      path_nm,
      read_group,
      start_date,
      end_date,
      batch_mk,
      generate_series(start_date, end_date, interval '1 day') as dt
    from
      (
        select
          file_batch_no,
          bucket_nm,
          path_nm,
          read_group,
          start_date,
          end_date,
          batch_mk
        from
          ami_dg.bucket_ctrl_log
        where
          proc_type = 0
          and enable_mk = 1
        order by
          crtd_time asc
        limit
          1
      ) a
  ) b;

select
  file_batch_no,
  bucket_nm,
  file_path,
  file_dir_ym,
  file_dir_date,
  read_group,
  batch_mk
from
  ami_dg.path_batch_log
where
  proc_type = 0
order by
  crtd_time asc
limit
  1;

select
  ami_ods.mdes_meterreading_upd_job(
    '${source}',
    '${meter_id}',
    '${read_time}',
    $ { version },
    '$ { source :equals(' HES - TMAP20210525 ') :ifElse(' lpi ', ' lpr ') }'
  );

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
