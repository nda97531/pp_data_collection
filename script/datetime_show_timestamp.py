from pp_data_collection.utils.time import str_2_timestamp

print(str_2_timestamp(
    str_time='2023/01/15 19:49:48.28',
    str_format='%Y/%m/%d %H:%M:%S.%f',
    tz=7
))
