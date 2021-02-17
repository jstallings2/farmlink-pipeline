import os
import pandas as pd 
import numpy as np 
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

# TODO: Scrape directly from USDA
# TODO: Skip the intermediate steps of saving as HTML and csv's

DIR = "./concat_data/"
JSON_DIR = "./json_data/"
test_regions = ['NORTHEAST+U.S.', 'SOUTHWEST+U.S.']
test_producenames = ['CARROTS', 'APPLES']

def load_and_clean(region, veg, dir):
    filepath = dir + region + "_" + veg + "_ALL.csv"
    try:
        df = pd.read_csv(filepath, parse_dates=True, index_col='Date')
    except FileNotFoundError:
        print("No data found for {} {}, skipping")
        return None, False
    
    # Drop null rows
    df.drop(df[df.index.isna() == True].index, inplace=True)
    print(sum(df.index.isna() == True))
    # Drop Unnamed column
    df.drop(['Unnamed: 0'], axis=1, inplace=True)
    return df, True

# Create the appropriate timedeltas
"""
Following function taken from https://stackoverflow.com/questions/765797/python-timedelta-in-years
Given the number of years ago from from_date, get the date that was exactly that many years ago
"""
def yearsago(years, from_date=None):
    if from_date is None:
        from_date = datetime.now()
    return from_date - relativedelta(years=years)

"""
Following function modified from https://stackoverflow.com/questions/765797/python-timedelta-in-years
Given the number of months ago from from_date, get the date that was exactly that many years ago
"""

def monthsago(months, from_date=None):
    if from_date is None:
        from_date = datetime.now()
    return from_date + relativedelta(months=(-months))

# Helper function for calculating % change
def pct_change(oldprice, newprice):
    return ((newprice - oldprice)/oldprice)*100



def calc_averages(region, veg, df, organic='both'):
    """
    Calculate the averages for a given region + veggie, organic, nonorganic, and all, and save 
    as 3 rows in a dataframe (will change to connect to firebase as well)

    Params:
        organic:    'only' => only organic
                    'no' => only non-organic
                    'both' => all will be calculated (organic status is ignored)

    Return true on successful execution
    """

    if organic == 'only':
        df = df[df['Organic'] == 'Y']
    elif organic == 'no':
        df = df[df['Organic'].isna()]

    # Get the data from today or most recent day with data
    current_day = datetime.today()
    orig_today = datetime(current_day.year, current_day.month, current_day.day)
    today = datetime(current_day.year, current_day.month, current_day.day)
    while not sum(df.index == today) > 0:
            today -= timedelta(days=1)
    if today != orig_today:
        print("No new data for {} {}, using most recent price from {}".format(veg, region, str(today)))
    today_df = df.loc[today]
    price_today = np.mean(today_df['Weighted Avg Price'])
    print("Price today: $" + str(round(price_today,2)))

    # Calculate 10 yr average
    df_10yr = pd.DataFrame()
    asterisk = False
    for i in range(10,-1,-1):
        year_exists = True
        date_back = yearsago(i, from_date=today)
        orig_date_back = date_back
        # Scoot back to the most recent date that contains data
        while not sum(df.index == date_back) > 0:
            date_back -= timedelta(days=1)
            if orig_date_back - date_back >= timedelta(days=365):
                print("No data for {} years back".format(i))
                asterisk = True
                year_exists = False
                break
        if year_exists:
            df_piece = df[df.index == date_back]
            df_10yr = pd.concat([df_10yr, df_piece], axis=0)
    print(df_10yr.index.value_counts())

    df_10yr_avg = df_10yr.groupby(df_10yr.index).mean()

    if df_10yr_avg.index[0] > yearsago(10, from_date=today):
        print("Data does not contain a price from 10 yrs ago for {} {}, using the earliest price point within 10 years that can find for pct change calculation".format(veg, region))
    price_10yr_ago = df_10yr_avg.iloc[0]['Weighted Avg Price']
    final_10yr_avg = np.mean(df_10yr_avg['Weighted Avg Price'])
    print(region, veg, "10 yr average: $" + str(round(final_10yr_avg, 2)))

    
    # Get data from closest to exactly 3 months ago as possible
    date_back_3mo = monthsago(3, from_date=today)
    while not sum(df.index == date_back_3mo) > 0:
        date_back_3mo -= timedelta(days=1)
    df_3mo = df[df.index == date_back_3mo]
    print(df_3mo)
    # Get data from closest to exactly 1 month ago as possible
    date_back_1mo = monthsago(1, from_date=today)
    while not sum(df.index == date_back_1mo) > 0:
        date_back_1mo -= timedelta(days=1)
    df_1mo = df[df.index == date_back_1mo]
    print(df_1mo)

    # Calculate 3mo and 1mo ago avg price
    final_3mo_avg = np.mean(df_3mo['Weighted Avg Price'])
    final_1mo_avg = np.mean(df_1mo['Weighted Avg Price'])
    print(region, veg, "3 months ago: $" + str(round(final_3mo_avg, 2)))
    print(region, veg, "1 months ago: $" + str(round(final_1mo_avg, 2)))

    # Calculate percent changes
    pct_change_10yr = pct_change(price_10yr_ago, price_today)
    pct_change_3mo = pct_change(final_3mo_avg, price_today)
    pct_change_1mo = pct_change(final_1mo_avg, price_today)
    if organic == 'only':
        org_str = 'Y'
    elif organic == 'no':
        org_str = 'N'
    else:
        org_str = 'B'


    new_cols = ['Date Added','Region', 'Commodity', 'Organic', '10yr_avg', '3mo_ago', '1mo_ago', 'pct_change (10yr)', 'pct_change (3mo)', 'pct_change (1mo)', 'price_today', '10_yr asterisk']
    vals = [current_day.strftime('%Y-%m-%d'), region, veg, org_str, final_10yr_avg, final_3mo_avg, final_1mo_avg, pct_change_10yr, pct_change_3mo, pct_change_1mo, price_today, asterisk]
    results_dict = dict(zip(new_cols, vals))
    print(results_dict)
    return results_dict


if __name__ == "__main__":
    for r in test_regions:
        for v in test_producenames:
            results_df = pd.DataFrame()
            input_df, status = load_and_clean(r, v, DIR)
            if not status:
                print("An error occured when loading data for: {} {}".format(r, v))
            else:
                both = calc_averages(r, v, input_df, organic='both')
                results_df = results_df.append(both, ignore_index=True)
                
                only = calc_averages(r, v, input_df, organic='only')
                results_df = results_df.append(only, ignore_index=True)
                
                no = calc_averages(r, v, input_df, organic='no')
                results_df = results_df.append(no, ignore_index=True)
            path = JSON_DIR + r + '_'+ v + '.json'
            results_df.to_json(path)
                
    










