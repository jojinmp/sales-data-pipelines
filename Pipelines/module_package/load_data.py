def json_parquet(sourcedir, targetdir):
    '''
    json_parquet is used to read source JSON files
    and convert it to partitioned parquet files.
    '''
    import pandas as pd
    from glob2 import glob
    import os
    import json
    import pyarrow as pa
    import pyarrow.parquet as pq
    from os.path import dirname
    import logging

    log = logging.getLogger(__name__)

    # Get the list of json files from dataset directory
    parent_dir = dirname(dirname(__file__))
    try:
        json_files = glob(parent_dir + '/' + sourcedir +'/*.json')
    except:
        log.error('Unable to read the source folder')
    df_list = []

    # Iterate through json files and converts data t0 pandas DF
    for json_file in json_files:
        with open(json_file, 'r') as f:
            data = json.loads(f.read())

            # Parse the nested json to create desired columns
            df = pd.json_normalize(data,
                                   meta=['ORDERNUMBER', 'PRODUCTCODE'],
                                   record_path=['attributes'])

        # Append individual DF to a list
        df_list.append(df)

    log.info('Data load is completed')
    # Create a single DF from list of DF's and converts to pyarrow table
    source_df = pd.concat(df_list, axis=0)

    # Convert ORDERDATE column to date format
    source_df['ORDERDATE'] = pd.to_datetime(source_df['ORDERDATE']).dt.date
    source_table = pa.Table.from_pandas(source_df)

    # Get absolute path for dataset folder
    dirname = os.path.dirname(__file__)
    parquet_directory = os.path.join(dirname, '..', sourcedir, targetdir)

    # Create partitioned Parquet data
    try:
        pq.write_to_dataset(source_table,
                            root_path= parquet_directory,
                            partition_cols=['ORDERDATE'])
    except:
        log.error('Error during write operation')
    
    log.info('Parquet write operation completed successfully')

def plot_diagram(plt):
    '''
    Function acceps current entity of plot
    and generate teh diagram in full screen
    '''
    # Set larger window size and plot the diagram
    plt.get_current_fig_manager().window.state('zoomed')
    plt.show()