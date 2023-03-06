from pp_data_collection.utils.time import str_2_timestamp

print(str_2_timestamp(
    str_time='2023/02/08 09:09:46.70',
    str_format='%Y/%m/%d %H:%M:%S.%f',
    tz=7
))
