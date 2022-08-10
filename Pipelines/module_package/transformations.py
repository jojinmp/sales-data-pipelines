import pandas as pd


def total_value(df, values):
    '''
    total_values takes status as a list and plot
    a bar chart against total value of each status.
    '''

    # Create total value dataset for provided status
    sum_df = df.groupby(['STATUS',
                         df.ORDERDATE.dt.year.rename('YEAR')], sort=False) \
        .agg({'SALES': 'sum'}) \
        .query('STATUS in @values').reset_index()
    pivot_df = sum_df.pivot(index='YEAR', columns='STATUS', values='SALES')

    return(pivot_df)


def product_per_line(df):
    '''
    product_per_line function takes sales dataset
    and returns count of unique products per line
    '''

    products = df.groupby('PRODUCTLINE')['PRODUCTCODE'].nunique()
    return products


def classic_cars_trend(df, values):
    '''
    classic_cars_trend function takes sales dataset
    and product line value input and return corresponding
    sales info per year-month
    '''
    # Convert date data to year_month and calculate total quntity ordered for each month
    df = df.query('STATUS == "Shipped"')
    classic_df = df.groupby(['PRODUCTLINE',
                             df.ORDERDATE.dt.to_period('M').rename('YEAR_MONTH')], sort=False) \
        .agg({'QUANTITYORDERED': 'sum'}) \
        .query('PRODUCTLINE in @values').reset_index()
    return classic_df


def discount(df, values):
    '''
    discount functio accepts source dataset and
    list of product lines to calcuate the total
    discount provided for each line over the years
    '''
    pd.options.mode.chained_assignment = None
    # Filter out unwanted data
    df = df.query('PRODUCTLINE in @values')
    # Call discount_calcluator function by passing rows to calculate discount amount
    df['DISCOUNT'] = df.apply(discount_calculator, axis=1)
    df = df.groupby(['PRODUCTLINE', df.ORDERDATE.dt.year.rename('YEAR')]) \
           .agg({'DISCOUNT': 'sum'}).unstack('YEAR').reset_index()
    return df


def discount_calculator(row):
    '''
    To calculate discount amount using
    Quantity ordered and MSRP
    '''
    dis = 0
    # Set quantity and msrp values
    quantity = row['QUANTITYORDERED']
    msrp = row['MSRP']
    if quantity <= 30:
        dis = 0
    if 30 < quantity <= 60:
        dis = msrp * 0.025 * quantity
    if 60 < quantity <= 80:
        dis = msrp * 0.04 * quantity
    if 80 < quantity <= 100:
        dis = msrp * 0.06 * quantity
    if quantity > 100:
        dis = msrp * 0.1 * quantity
    return(dis)
