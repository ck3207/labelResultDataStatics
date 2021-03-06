# -*- coding: utf-8 -*-
import re
import pickle
import os

__author__ = "chenk"


class ExtremeValue:
    FILTER_TABLES = []
    FILTER_COLUMNS = ["fund_account", "client_id", "init_date", "client_name", "open_time", "open_date1",
                      "age_title", "corp_risk_level", "fundacct_status", "client_rights", "mobile_tel",
                      "branch_no", "belong_branch", "open_branch_area", "company_name", "client_gender",
                      "part_init_date", "interval_type", "activity", "stock_code", "prod_code", "prodta_no",
                      "stock_type", "exchange_type", "business_flag", "money_type", "score_type"]
    COLUMNS = ["fund_account", "interval_type", "part_init_date"]

    def __init__(self, part_init_date, columns_file, extreme_value_file, distribution_statistics_file):
        self.get_columns_file = columns_file
        self.get_extreme_value_file = extreme_value_file
        self.distribution_statistics_file = distribution_statistics_file
        self.table_columns = {}
        self.sql_models = {}
        self.part_init_date = part_init_date
        self.results = {}
        self.distribution_statistics = {}   #
        self.distribution_statistics_pkl = "distribution_statistics_model.pkl"  # SQL Model
        self.data_pkl = "data.pkl"  # Extreme Value

    def get_tables_from_file(self, file_name="tables.txt"):
        tables = []
        with open(file=file_name, mode="r", encoding="utf-8") as f:
            while True:
                line = f.readline()
                if line == "":
                    break
                table = line.replace("\n", "").strip()
                tables.append(table)

        return tables

    def generate_get_columns_hive_sql(self, table_file="tables.txt", keyword="acct_wt_", file_name="get_columns.sql"):
        """Organize and generate a description SQL file."""
        # tables = ExtremeValue.JOBS
        self.f_get_columns = open(file_name, mode="w", encoding="utf-8")
        tables = self.get_tables_from_file(file_name=table_file)
        for job in tables:
            if job.startswith(keyword):
                job = "wt_customer." + job
                content = self.get_description_sql(job)
                self.write_to_file(f=self.f_get_columns, content=content, need_transfer=True)
                self.write_to_file(f=self.f_get_columns, content="select '{0}';\n".format("="*20+"table分割线"+"="*20))
            else:
                print("The Job[%s] was not matched." % job)
        self.f_get_columns.close()

    def get_description_sql(self, table_name):
        """Stucture a description SQL."""
        # sql = "select 'desc {0}';".format(table_name)
        sql = "desc {0};".format(table_name)
        return sql

    def extract_columns_from_log(self, file_name="jobs.log"):
        """Extract the columns which was describing on previous step."""
        columns = []
        with open(file_name, mode="r", encoding="utf-8") as f:
            for line in f.readlines():
                if line.startswith("desc"):
                    table_name = line.split(" ")[-1][:-1]
                elif line.startswith("="*5) and "table分割线" in line:
                    self.table_columns[table_name] = columns
                    columns = []
                elif line.startswith("#"):
                    continue
                else:
                    try:
                        column = self.__get_column(line)
                    except AttributeError:
                        column = ""
                    if column in columns or not column:
                        continue
                    columns.append(column)
        return self.table_columns

    def extract_extreme_value_from_log(self, file_name="get_extreme.log"):
        """Extract value from log file."""
        table, column = "", ""
        f = open(file=file_name, mode="r", encoding="utf-8")
        insert_data = ""
        while True:
            line = f.readline()
            if line == "":
                f.close()
                break
            if "TableNum" in line:
                insert_data = "num"
                num = 0
                f.readline()
                continue
            if insert_data == "num":
                num = int(line.strip())
                insert_data = ""
                continue

            if not table or not column:
                if line.startswith("--"):
                    db_table_column = line.split(" ")[-1].strip()
                    db, table, column = db_table_column.split(".")
                    if not self.results.get(table):
                        self.results[table] = {column: {}, "num": num}
                    # column = re.search("([\w_]*) as", f.readline()).group(1)
                    self.results[table][column] = {"max": [], "min": [], "null": []}
                    insert_data = "max"
                continue
            if "column_split" in line or "table_split" in line:
                table, column = "", ""
                continue
            if "select" in line:
                if "as max_value" in line:
                    insert_data = "max"
                elif "as min_value" in line:
                    insert_data = "min"
                elif "is null" in line:
                    insert_data = "null"
                continue
            if "select" not in line and "=====" not in line:
                self.__extract_value(table, column, line, insert_data)
        self.dump_data_to_pickle(self.results)
        # print("self.results: \n", self.results)
        return self.results

    def __extract_value(self, table, column, line, data):
        if data == "null":
            self.results[table][column][data].append(line.replace("\n", "").replace("\t", ","))
        else:
            self.results[table][column][data].append(line.replace("\n", "").split("\t"))

    def __get_column(self, line):
        """Get the first column from log file. For example, the line is like:
        init_date           	string              	????                
        client_id           	string              	??ID                
        fund_account        	string              	????                
        client_name         	string              	????                
        open_date           	string              	????                
        open_time           	string              	?????????  
        
        When calling the function, the argue line is one of them. 
        And, the function return the first column of line(init_date, clinet_id, fund_account and so on).
        """
        return re.match("[a-zA-Z_0-9]+", line).group(0)

    def write_to_file(self, content, f, need_transfer=False):
        """Write to content to a file."""
        query_split = "=" * 20 + "query_split" + "=" * 20
        if isinstance(content, list):
            for _content in content:
                _content = _content.replace("'--'", '"""--"""')
                if need_transfer:
                    self.__write_to_file(f, _content)
                f.write(_content+"\n")
                self.__write_to_file(f=f, content=query_split)
        elif isinstance(content, str):
            _content = content.replace("'--'", '"""--"""')
            if need_transfer:
                self.__write_to_file(f, _content)
            f.write(_content + "\n")
            self.__write_to_file(f=f, content=query_split)

    def __write_to_file(self, f, content):
        """Write to content to a file. However, content will be selected not executed ."""
        sql_comment = "select '{0}';".format(content.replace(";", ""))
        f.write(sql_comment+"\n")

    @staticmethod
    def is_filter_column(column):
        if column in ExtremeValue.FILTER_COLUMNS:
            return True
        return False

    @staticmethod
    def is_filter_table(table):
        if table in ExtremeValue.FILTER_TABLES:
            return True
        return False

    def close_file(self, f):
        """Close file which was opened."""
        f.close()

    def generate_get_extreme_value_sql(self, file_name, null_sql_flag=True, fund_accounts=None):
        """file_name为 已获取表字段的 日志文件。 即：desc table 的结果文件;
        null_sql_flag 为 False 时， 不构建 获取字段为null的SQL，
        null_sql_flag 为 True 时， 构建 获取字段为null的SQL;
        """
        f_get_extreme_value = open(file_name, mode="w", encoding="utf-8")
        for table, columns in self.table_columns.items():
            # table_type = self.__distinguish_type(table)
            self.generate_table_count_sql(f=f_get_extreme_value, table_name=table)
            if ExtremeValue.is_filter_table(table):
                continue
            self.distribution_statistics[table] = {}  # 统计数据模板构造
            for column in columns:
                if ExtremeValue.is_filter_column(column):
                    continue
                self.distribution_statistics[table][column] = []    # 统计数据模板构造
                self.__write_to_file(f=f_get_extreme_value, content="-- {0}.{1}".format(table, column))
                # get target sql of extreme value
                extreme_value_sql = self.__get_extreme_value_sql(table=table, column=column, fund_accounts=fund_accounts)
                # write sql to a file
                self.write_to_file(content=extreme_value_sql, f=f_get_extreme_value, need_transfer=True)
                if null_sql_flag:
                    # get sql which to judge whether there is a null value or not
                    null_sql = self._get_is_null_sql(table=table, column=column)
                    # write sql to a file
                    self.write_to_file(content=null_sql, f=f_get_extreme_value, need_transfer=True)
                self.__write_to_file(f=f_get_extreme_value, content="=" * 20 + "column_split" + "=" * 20)
            self.__write_to_file(f=f_get_extreme_value, content="=" * 20 + "table_split" + "=" * 20)
        self.dump_data_to_pickle(data=self.distribution_statistics, filename=self.distribution_statistics_pkl)
        # print("self.distribution_statistics:\n", self.distribution_statistics)
        f_get_extreme_value.close()

    def __get_extreme_value_sql(self, table, column, fund_accounts):
        sql_models = []  # 根据字段生成的SQL 列表
        exist_columns = self.__is_columns_exist(table=table, columns=ExtremeValue.COLUMNS)
        interval_type_flag = exist_columns.get("interval_type")
        self.__get_distribution_statistics_sql_model(table=table, column=column, exist_columns=exist_columns)
        for max_or_min in ["max", "min"]:
            sort = "asc"
            if max_or_min == "max":
                sort = "desc"
            for interval_type in [1, 2, 3, 4]:
                sql_model = ""
                if exist_columns.get("fund_account"):
                    sql_model += "select fund_account, {0} as {1}_value, ".format(column, max_or_min)
                else:
                    sql_model += "select '--' as fund_account, {0} as {1}_value, ".format(column, max_or_min)
                if exist_columns.get("interval_type"):
                    sql_model += "interval_type from {0} ".format(table)
                else:
                    sql_model += "'--' as interval_type from {0} ".format(table)
                if exist_columns.get("part_init_date"):
                    sql_model += "where part_init_date = {0} ".format(self.part_init_date)
                    if exist_columns.get("interval_type"):
                        sql_model += "and interval_type = {0} ".format(interval_type)
                elif exist_columns.get("interval_type"):
                    sql_model += "where interval_type = {0} ".format(interval_type)
                if exist_columns.get("fund_account") and fund_accounts:
                    # 处理 fund_accounts 为多个与单个的情况
                    filter_fund_account_sql = ""
                    if isinstance(fund_accounts, list):
                        filter_fund_account_sql += "fund_account in ("
                        for fund_account in fund_accounts:
                            filter_fund_account_sql += "'{0}',".format(fund_account)
                        filter_fund_account_sql += filter_fund_account_sql[:-1] + ") "
                    elif isinstance(fund_accounts, str):
                        filter_fund_account_sql += "fund_account = '{0}' ".format(fund_accounts)
                    # 处理拼接语句 存在 where 条件与不存在 where 条件的情况
                    if "where" in sql_model:
                        sql_model += "and " + filter_fund_account_sql
                    else:
                        sql_model += "where " + filter_fund_account_sql
                sql_model += "order by {0} {1} limit 1;".format(column, sort)
                sql_models.append(sql_model)
                if not interval_type_flag:  # 这里看起来有bug
                    break
        return sql_models

    def generate_distribution_statistics_sql(self, file_name):
        """根据分段统计数据的模板， 生成实际的执行sql"""
        f_distribution_statistics = open(file=file_name, mode="w", encoding="utf-8")
        sql_models = dict(self.load_data_from_pickle(self.distribution_statistics_pkl))
        models = []
        for table in sql_models.keys():
            for column in sql_models.get(table).keys():
                self.__write_to_file(content="-- {0}.{1}".format(table, column), f=f_distribution_statistics)
                data_index = 0
                for sql in sql_models.get(table).get(column):
                    data_index += 1
                    is_first = True  # 判断是不是第一次生成执行sql
                    for extreme_values in self.__get_columns_value(table, column, data_index):
                        if is_first:
                            origin_sql = sql
                            sql = sql.replace(">", ">=")
                            is_first = False
                        else:
                            sql = origin_sql
                        min_value, max_value = extreme_values
                        execute_sql = sql.format(min_value, max_value)
                        models.append(execute_sql)
                        self.write_to_file(content=execute_sql, f=f_distribution_statistics, need_transfer=True)
                self.__write_to_file(content="=" * 20 + "column_split" + "=" * 20, f=f_distribution_statistics)
            self.__write_to_file(content="=" * 20 + "table_split" + "=" * 20, f=f_distribution_statistics)

        f_distribution_statistics.close()

    def __get_columns_value(self, table, column, data_index, file_name="data.pkl", pieces=10):
        """获取每个时间维度interval_type的极大值与极小值，并平均分割若干组数据，以数组返回。
        table 为 表； column 为 表字段, 格式为：wt_customer.table， 即库名.表名；
        data_index 为interval_type的实际值，当前支持 [1, 2, 3, 4]
        file_name 为 之前已存的极值数据的pkl文件
        pieces 为分成数据的组数，默认为10份
        """
        columns_value = dict(self.load_data_from_pickle(file_name))
        # print("Columns Value:\n", columns_value)
        #  {'acct_wt_user_max_loss_profit': {'max_loss': {'min': [['10001102', '0', '--']], 'null': [],
        #  'max': [['10700183', '12127', '--']]}}}
        try:
            min_value = float(columns_value.get(table.split(".")[1]).get(column).get("min")[data_index-1][1])
            max_value = float(columns_value.get(table.split(".")[1]).get(column).get("max")[data_index-1][1])
        except Exception as e:
            print("Error:table:%s, column:%s, data_index: %d" % (table, column, data_index))
        return self.__get_pieces(min_value=min_value, max_value=max_value, pieces=pieces)

    def __get_pieces(self, min_value, max_value, pieces):
        """根据入参的最大最小值，平均划分成 若干组数据"""
        pieces_list = []
        value = min_value
        if isinstance(min_value, float) and isinstance(max_value, float):
            differentials = max_value - min_value
            differentials_piece = differentials / pieces
        for i in range(pieces):
            pieces_list.append((value, round(value + differentials_piece, 6)))
            value = round(value+differentials_piece, 6)

        return pieces_list

    def __get_distribution_statistics_sql_model(self, table, column, exist_columns):
        """根据表字段信息，生成一个获取分段统计数据的模板
        eg：select count(1) from table where part_init_date = xxx 
        and interval_type = 1 and column value between 1 and 1000; """
        self.distribution_statistics[table][column] = []
        init_sql = "select count(1) from {0} where ".format(table)
        sql_model = init_sql
        if exist_columns.get("part_init_date"):
            sql_model += "part_init_date = {0} and ".format(self.part_init_date)
        if exist_columns.get("interval_type"):
            sql_model += "interval_type = 1 and "
        sql_model += "%s > {0} and %s <= {1} ;" % (column, column)
        self.distribution_statistics[table][column].append(sql_model)
        if exist_columns.get("interval_type"):
            for interval_type in ["2", "3", "4"]:
                tmp_sql = sql_model.replace("interval_type = 1", "interval_type = {0}".format(interval_type))
                self.distribution_statistics[table][column].append(tmp_sql)

        return

    def _get_is_null_sql(self, table, column):
        sql_models = []
        exist_columns = self.__is_columns_exist(table=table, columns=ExtremeValue.COLUMNS)
        for interval_type in [1, 2, 3, 4]:
            sql_model = ""
            if exist_columns.get("fund_account"):
                sql_model += "select fund_account, null as value, "
            else:
                sql_model += "select '--' as fund_account, null as value, "
            if exist_columns.get("interval_type"):
                sql_model += "interval_type from {0} ".format(table)
                interval_type_info = "and interval_type = {0} ".format(interval_type)
            else:
                sql_model += "'--' as interval_type from {0} ".format(table)
                interval_type_info = " "
            if exist_columns.get("part_init_date"):
                sql_model += "where part_init_date = {0} {1} and {2} is null limit 1;".\
                    format(self.part_init_date, interval_type_info, column)
            elif exist_columns.get("interval_type"):
                sql_model += "where {0} and {1} is null limit 1;".format(interval_type_info, column)
            else:
                sql_model += "where {0} is null limit 1;".format(column)
            sql_models.append(sql_model)
            if not exist_columns.get("interval_type"):
                break

        return sql_models

    def generate_table_count_sql(self, f, table_name):
        sql = self.__get_table_count_sql(table_name=table_name)
        self.__write_to_file(content="-- TableNum:{0}".format(table_name), f=f)
        # self.__write_to_file(f=f, content="=" * 20 + "query_split" + "=" * 20)
        self.write_to_file(content=sql, f=f, need_transfer=True)


    def __get_table_count_sql(self, table_name):
        """Generate a sql which would know how many data in table."""
        sql_model = "select count(1) from {0} ".format(table_name)
        exist_columns = self.__is_columns_exist(table=table_name, columns=ExtremeValue.COLUMNS)
        if exist_columns.get("part_init_date"):
            sql_model += "where part_init_date = {0};".format(self.part_init_date)
        else:
            sql_model += ";"
        return sql_model

    def __distinguish_type(self, table):
        """Distinguish table and return a type for function get_extreme_value_sql_model argue type.
        table_type = 0, a full sql model. It contain fund_account, max/min value and interval_type.
        table_type = 1, there is no fund_account. And fund_account is replaced by characters '--'.
        table_type = 2, there is no interval_type. And interval_type is replaced by characters '--'.
        table_type = 3, there is no interval_type and fund_account. 
        And interval_type and fund_account are replaced by characters '--'.
        """
        if "fund_account" in self.table_columns[table]:
            if "interval_type" in self.table_columns[table]:
                return 0
            else:
                return 2
        elif "interval_type" in self.table_columns[table]:
            return 1
        else:
            return 3

    def __is_columns_exist(self, table, columns=[]):
        is_exist = {}
        for column in columns:
            is_exist[column] = False
            if column in self.table_columns[table]:
                is_exist[column] = True
        return is_exist

    def dump_data_to_pickle(self, data, filename="data.pkl"):
        """Dump data to pickle file."""
        with open(file=filename, mode="wb") as f:
            pickle.dump(data, f)

    def load_data_from_pickle(self, filename="data.pkl"):
        """Load Data From Pickle File."""
        with open(file=filename, mode="rb") as f:
            return pickle.load(f)


def get_config(sec):
    import configparser
    cf = configparser.ConfigParser()
    cf.read(filenames="config.ini", encoding="utf-8")
    if sec not in cf.sections():
        raise Exception("Section {0} not in config.ini".format(sec))
    else:
        return cf


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
    null_sql_flag = bool(config.get(section=sec, option="null_sql_flag"))
    fund_account = config.get(section=sec, option="fund_account")
    if "," in fund_account:
        fund_accounts = fund_account.strip().split(",")
    else:
        fund_accounts = fund_account.strip()

    extreme_value = ExtremeValue(part_init_date, columns_file, extreme_file, distribution_statistics_file)
    # extreme_value.generate_get_columns_hive_sql(table_file=table_file, file_name=columns_file)
    extreme_value.extract_columns_from_log(file_name=columns_file.replace(".sql", ".log"))
    extreme_value.generate_get_extreme_value_sql(file_name=extreme_file,
                                                 null_sql_flag=null_sql_flag,
                                                 fund_accounts=fund_accounts)
    # extreme_value.extract_extreme_value_from_log(file_name=extreme_file.replace(".sql", ".log"))
    # extreme_value.generate_distribution_statistics_sql(file_name=distribution_statistics_file)
