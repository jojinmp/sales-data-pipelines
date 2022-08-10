import os
import pyarrow.parquet as pq
import pandas as pd
import matplotlib.pyplot as plt

from module_package import load_data
from module_package import transformations

import logging
logging.basicConfig(filename="log.txt", format='%(asctime)s %(message)s', \
                    datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
logging.info("New execution started")

source_dir = 'dataset'
target_dir = 'output'

# Call json_parquet function to read JSON and to create parquet files
load_data.json_parquet(source_dir, target_dir)
logging.info("Data set loading is completed")

# Get absolute path of parquet directory
dirname = os.path.dirname(__file__)
parquet_directory = os.path.join(dirname, source_dir, target_dir)

# Read parquet dataset
try:
    dataset = pq.ParquetDataset(parquet_directory) 
    table = dataset.read()
    source_df = table.to_pandas()
    source_df['ORDERDATE'] = pd.to_datetime(source_df['ORDERDATE'])
except:
    logging.info('Unable to load parquet data')

logging.info('Parquet data is loaded')
# Set status values and calls function to get total value data
values = ['Cancelled','On Hold']
data_df = transformations.total_value(source_df, values)
sales_loss = data_df.plot(kind = 'bar', title = 'Total (Cancelled, On Hold) Sales Per Year', 
                          xlabel = 'Year', ylabel = 'Sales', rot = 0, subplots = True)
load_data.plot_diagram(plt)
logging.info("Sales data plot is completed")

# Call function to get products per line data
product_df = transformations.product_per_line(source_df)
product_count = product_df.plot(kind = 'bar', title = 'Products per line', 
                                xlabel = 'Product Line', ylabel = 'Number of Products', rot = 0)
load_data.plot_diagram(plt)
logging.info("Product line plot is completed")

# Get the classic car trend data
product_lines = ['Classic Cars']
trend_df = transformations.classic_cars_trend(source_df, product_lines)
trend = trend_df.plot(kind = 'line', title = 'Classic Car Trend', x = 'YEAR_MONTH', 
                      xlabel = 'Year_Month', ylabel = 'Quantity Ordered')
load_data.plot_diagram(plt)
logging.info("Classic car plot is completed")

# Call discount function to get the related trend for given product lines
values = ['Classic Cars', 'Vintage Cars', 'Motorcycles', 'Trucks and Buses']
discount_df = transformations.discount(source_df, values)
discount = discount_df.plot(kind = 'bar', title = 'Discount on Products', x = 'PRODUCTLINE',
                            y = 'DISCOUNT', xlabel = 'Product Line', ylabel = 'Discount', rot = 0)
load_data.plot_diagram(plt)
logging.info("Discount plot is completed")

logging.info("Execution completed")
