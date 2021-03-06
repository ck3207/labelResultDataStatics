# -*- coding: utf-8 -*-
import os

from extreme_value import ExtremeValue, get_config

__author__ = "chenk"


if __name__ == "__main__":
    try:
        sec = os.sys.argv[1]
    except IndexError as e:
        sec = "default"

    # get config info
    config = get_config(sec)
    part_init_date = int(config.get(section=sec, option="part_init_date"))
    columns_file = config.get(section=sec, option="columns_file")
    extreme_file = config.get(section=sec, option="extreme_file")
    table_file = config.get(section=sec, option="jobs")
    distribution_statistics_file = config.get(section=sec, option="distribution_statistics_file")
    null_sql_flag = config.get(section=sec, option="null_sql_flag")
    fund_account = config.get(section=sec, option="fund_account")

    extreme_value = ExtremeValue(part_init_date, columns_file, extreme_file, distribution_statistics_file)
    extreme_value.extract_columns_from_log(file_name=columns_file.replace(".sql", ".log"))
    extreme_value.generate_get_extreme_value_sql(file_name=extreme_file)
