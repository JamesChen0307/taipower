from mp.py import ThreadWorker


@ThreadWorker(tasks=impute.iterrows(), thread=10, args=(1, 2, 3), kwargs={"a": 1, "b": 2})
def func(task, *args, **kwargs):
    a, b, c, *_ = args
    kwargs['a']

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

    # 否定句
    if lp_result is not None:
        continue


    selected_row = impute.loc[index]
    json_data = selected_row.to_json()
    redis_conn.execute_command("JSON.SET", lpdata_key, ".", json_data)

func()

