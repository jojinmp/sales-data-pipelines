import pytest
import pyarrow.parquet as pq
import os
from os.path import dirname
import pandas as pd
import json

import sys

# To resolve import module issues
myPath = dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + '/../')
from module_package import load_data, transformations

source_dir = 'test_data'
target_dir = 'output'
parent_dir = dirname(dirname(__file__))

def test_json_parquet():
    # Read json file and calculate the row count
    test_file1 = open(parent_dir + '/' + source_dir + '/sample_data_2003.json' )
    test_data1 = json.load(test_file1)
    test_file2 = open(parent_dir + '/' + source_dir + '/sample_data_2004.json' )
    test_data2 = json.load(test_file2)
    total_count = len(test_data1) + len(test_data2)
    # Call functon for creating parquet files
    load_data.json_parquet(source_dir, target_dir)
    # Read parquet file and compare count
    parquet_directory = os.path.join(parent_dir, source_dir, target_dir)
    dataset = pq.ParquetDataset(parquet_directory)
    table = dataset.read()
    global source_df
    source_df = table.to_pandas()
    source_df['ORDERDATE'] = pd.to_datetime(source_df['ORDERDATE'])
    assert total_count == source_df.shape[0]

# Read parquet data and store in a variable to avoid multiple reads
# parquet_directory = os.path.join(parent_dir, source_dir, target_dir)
# dataset = pq.ParquetDataset(parquet_directory)
# table = dataset.read()
# source_df = table.to_pandas()
# source_df['ORDERDATE'] = pd.to_datetime(source_df['ORDERDATE'])

def test_total_value():
    values = ['Cancelled','On Hold']
    data_df = transformations.total_value(source_df, values)
    sales = data_df.query('YEAR==2003')['On Hold'].values[0]
    assert sales == 3729.39

def test_product_per_line():
    product_df = transformations.product_per_line(source_df)
    assert product_df[0] == 2

# To remove the parquet directory which may create errors in subsequent runs
def test_remove_output_directory():
    import shutil
    shutil.rmtree(parent_dir + '/' + source_dir + '/output')
    assert 1 == 1
