from pp_data_collection.utils.time import str_2_timestamp

print(str_2_timestamp(
    str_time='2022/12/22 09:03:45.33',
    str_format='%Y/%m/%d %H:%M:%S.%f',
    tz=7
))
