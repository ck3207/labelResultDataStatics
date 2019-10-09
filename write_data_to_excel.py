# -*- coding: utf-8 -*-
import xlwt
import pickle

__author__ = "chenk"


class WriteDataToExcel:
    """Read data and write to excel."""
    def __init__(self, book_name="CaiDaDataAnalyse.xls"):
        self.book_name = book_name
        self.wt = xlwt.Workbook()
        self.table = self.wt.add_sheet("data")
        self.x = 0
        self.y = 0
        self.absolutely_pos_x = 0
        self.absolutely_pos_y = 0

    def deal_data(self, data={}):
        """Read data, and write to excel file.
        You can see the final result via http://note.youdao.com/noteshare?
        id=e201120deca6b428e8ebfc9be1672ac3&sub=36CB2F77F63942ABBE08A668C378C101"""
        print(data)
        for table, info in data.items():
            if "acct_wt_user_stock_select_score" in table:
                print("Let's Debug.")
            # table information
            self.write_data("table_name", {"x": self.x, "y": self.y}, flag=True)
            self.write_data(table, {"x": 1, "y": 0}, flag=False)
            self.write_data("num", {"x": 1, "y": 0}, flag=False)
            temp = data[table]["num"]
            if isinstance(temp, int):
                self.write_data(temp, {"x": 1, "y": 0})
            else:
                self.write_data("暂不统计", {"x": 1, "y": 0})

            self.absolutely_pos_y += 1
            self.x = self.absolutely_pos_x
            self.y = self.absolutely_pos_y

            # column fields
            for column_field in ["column_name", "fund_account", "max_value", "interval_type",
                                 "fund_account", "min_value", "interval_type", "NULL_value", "remark"]:
                self.write_data(column_field, {"x": self.x, "y": self.y}, flag=True)
                self.x += 1

            self.absolutely_pos_y += 1
            self.x = self.absolutely_pos_x
            self.y = self.absolutely_pos_y

            for column, value_info in info.items():
                if column == "er":
                    pass
                # column fields
                if column == "num":
                    continue
                # value fields
                self.write_data(column, {"x": self.x, "y": self.y}, flag=True)
                self.x += 1

                self.absolutely_pos_x = self.x
                self.absolutely_pos_y = self.y

                temp_y = self.y  # if there is no data, then temp_y will be equal to self.y
                max_y = self.y  # if there is part of intervals is no data, then max_y is the new absolutely_y position.
                for key in ["max", "min", "null"]:
                    # When key is null, then dealing specially.
                    if key == "null" and len(data[table][column][key]) == 0:
                        continue
                    self.y = self.absolutely_pos_y
                    for value in data[table][column][key]:
                        self.x = self.absolutely_pos_x
                        # When key is null, then dealing specially.
                        if key == "null":
                            self.write_data(value, {"x": self.x, "y": self.y}, flag=True)
                            self.y += 1
                            max_y = max(max_y, self.y)
                            continue
                        for i, column_value in enumerate(value):
                            self.write_data(column_value, {"x": self.x, "y": self.y}, flag=True)
                            self.x += 1
                        self.y += 1
                        max_y = max(max_y, self.y)
                    self.absolutely_pos_x = self.x
                # deal with the table is no data
                if temp_y == max_y:
                    self.y += 1
                # may be there is part of intervals is no data
                else:
                    self.y = max_y
                self.absolutely_pos_y = self.y
                self.x, self.absolutely_pos_x = 0, 0
        return self.wt.save(self.book_name)

    def write_data(self, value, add_index={"x": 0, "y": 0}, flag=False):
        """Write data to a cell. When flag is True, the cell means absolute position. (add_index["x], add_index["y"]) 
        When the flag is False, the cell is relative position. (add_index["x"]+x, add_index["y"]+y)"""
        value = str(value)
        if flag:
            print(add_index["y"], add_index["x"], value)
            self.table.write(add_index["y"], add_index["x"], value)
            return
        else:
            self.x += add_index["x"]
            self.y += add_index["y"]
            print(self.y, self.x, value)
            # if self.x
            self.table.write(self.y, self.x, value)

    def add_index(self, add_value={"x": 0, "y": 0}):
        pass
        # self.x += add_value["x"]
        # self.y += add_value["y"]

    def load_data_from_pickle(self, filename="data.pkl"):
        """Load Data From Pickle File."""
        with open(file=filename, mode="rb") as f:
            return pickle.load(f)


if __name__ == "__main__":
    write_data_to_excel = WriteDataToExcel(book_name="zhongyou.xls")
    data = write_data_to_excel.load_data_from_pickle(filename="data.pkl")
    write_data_to_excel.deal_data(data)

