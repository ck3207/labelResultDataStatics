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

    extreme_value = ExtremeValue(part_init_date, columns_file, extreme_file, distribution_statistics_file)
    extreme_value.generate_get_columns_hive_sql(table_file=table_file, file_name=columns_file)
