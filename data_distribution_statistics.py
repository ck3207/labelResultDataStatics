# -*- coding: utf-8 -*-
import pickle

__author__ = "chenk"


class DistributionStatistics:
    """数据分布统计"""
    def __init__(self, data_file="data.pkl"):
        self.data_file = data_file

    def load_data_from_pickle(self, filename="data.pkl"):
        """Load Data From Pickle File."""
        with open(file=filename, mode="rb") as f:
            return pickle.load(f)