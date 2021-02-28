import os
import requests
import pandas as pd 
import numpy as np 
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json

DIR = "./concat_data/"
JSON_DIR = "./json_data/"
test_regions = ['NORTHEAST+U.S.', 'SOUTHWEST+U.S.']
test_producenames = ['CARROTS', 'APPLES']

def get_current_date():
    today = datetime.now()
    return today.month, today.day, today.year

def update_data(regionname, producename, savedir='./concat_data/', test=False):
    """
    Performs the web scrape to grab the newest data for a given region and veggie,
    and combines it with the old data for that region & veggie and returns the
    combined dataframe with data +11 years ago cut off, and also OVERWRITES the backup csv of the old data
    """
    if test:
        old_df = load_and_clean(regionname, producename)[0]
        return old_df

    new_df = fetch_data(producename, regionname)[0]
    old_df = load_and_clean(regionname, producename)[0]
    print('Old data:\n', old_df.tail(5))
    print('New data:\n', new_df.head(5))
    combined_df = pd.concat([old_df, new_df], ignore_index=False)
    # Chop off the data more than 11 years prior 11 year data might 
    # still be needed for 10yr average if the dates dont line up
    combined_df = combined_df[combined_df.index > yearsago(11)]

    # Save combined_df
    filepath = savedir + str(regionname) + '_' + str(producename) + '_ALL.csv'
    combined_df.to_csv(filepath)

    # Return combined_df
    return combined_df

def fetch_data(producename, regionname):
    """Given a region and produce item, fetches a year of data and MAKES A DATAFRAME OF IT.
    Skips any cities/items/year combos that have already been downloaded. Slightly hardened against 
    timeouts,etc. from the USDA server, which is a bit flaky.
    """
    month, day, year = get_current_date()

    url = 'https://www.marketnews.usda.gov/mnp/fv-report-retail?repType=&run=&portal=fv&locChoose=&commodityClass=&startIndex=1&type=retail&class=ALL&commodity='+str(producename)+'&region='+str(regionname)+'&organic=ALL&repDate='+str(month)+'%2F'+str(month)+'%2F'+str(year)+'&endDate=12%2F31%2F'+str(year)+'&compareLy=No&format=excel&rowDisplayMax=100000'

    try:
        r = requests.get(url, allow_redirects=True, timeout=300)
        new_data = pd.read_html(r.content, header=0, parse_dates=True, index_col='Date')[0]
        return new_data, True
    except requests.exceptions.Timeout:
        print('request timed out, trying again...')
        try:
            r = requests.get(url, allow_redirects=True, timeout=300)
            new_data = pd.read_html(r.content, header=0, parse_dates=True, index_col='Date')[0]
            return new_data, True
        except requests.exceptions.Timeout:
            print('request timed out again, exiting...')
            return None, False

def load_and_clean(region, veg, dir='./concat_data/'):
    filepath = dir + region + "_" + veg + "_ALL.csv"
    try:
        df = pd.read_csv(filepath, parse_dates=True, index_col='Date')
    except FileNotFoundError:
        print("No data found for {} {}, skipping")
        return None, False
    
    #Drop null rows
    if sum(df.index.isna() == True) > 0:
        df.drop(df[df.index.isna() == True].index, inplace=True)
    # Drop Unnamed column
    if 'Unnamed: 0' in df.columns:
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



def calc_averages(region, veg, df, adjusted=True):
    """
    Calculate the averages for a given region + veggie, organic, nonorganic, and all, and save 
    as 3 rows in a dataframe (will change to connect to firebase as well)

    Params:
        region: Region to caculate for
        veg: Commodity to calculate for
        df: DataFrame to use for the calculations

    Return true on successful execution
    """

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
    if adjusted:
        adj_price_today = np.mean(today_df['IA Avg Price'])
        print("Price today (adjusted for inflation): $" + str(round(adj_price_today,2)))

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
        asterisk = True
    price_10yr_ago = df_10yr_avg.iloc[0]['Weighted Avg Price']
    final_10yr_avg = np.mean(df_10yr_avg['Weighted Avg Price'])
    print(region, veg, "10 yr average: $" + str(round(final_10yr_avg, 2)))
    if adjusted:
        adj_price_10yr_ago = df_10yr_avg.iloc[0]['IA Avg Price']
        adj_final_10yr_avg = np.mean(df_10yr_avg['IA Avg Price'])
        print(region, veg, "adj 10 yr average: $" + str(round(adj_final_10yr_avg, 2)))
    
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
    if adjusted:
        adj_final_3mo_avg = np.mean(df_3mo['IA Avg Price'])
        adj_final_1mo_avg = np.mean(df_1mo['IA Avg Price'])
        print(region, veg, "adj 3 months ago: $" + str(round(adj_final_3mo_avg, 2)))
        print(region, veg, "adj 1 months ago: $" + str(round(adj_final_1mo_avg, 2)))

    # Calculate percent changes
    if adjusted:
        pct_change_10yr = pct_change(adj_price_10yr_ago, adj_price_today)
        pct_change_3mo = pct_change(adj_final_3mo_avg, adj_price_today)
        pct_change_1mo = pct_change(adj_final_1mo_avg, adj_price_today)
    else:
        pct_change_10yr = pct_change(price_10yr_ago, price_today)
        pct_change_3mo = pct_change(final_3mo_avg, price_today)
        pct_change_1mo = pct_change(final_1mo_avg, price_today)


    new_cols = ['Date Added','Region', 'Commodity', '10yr_avg', '3mo_ago', '1mo_ago', 'pct_change (10yr)', 'pct_change (3mo)', 'pct_change (1mo)', 'price_today', '10_yr asterisk', 'adjusted']
    if adjusted:
        vals = [current_day.strftime('%Y-%m-%d'), region, veg, adj_final_10yr_avg, adj_final_3mo_avg, adj_final_1mo_avg, pct_change_10yr, pct_change_3mo, pct_change_1mo, adj_price_today, asterisk, True]
    else:
        vals = [current_day.strftime('%Y-%m-%d'), region, veg, adj_final_10yr_avg, adj_final_3mo_avg, adj_final_1mo_avg, pct_change_10yr, pct_change_3mo, pct_change_1mo, adj_price_today, asterisk, True]
    results_dict = dict(zip(new_cols, vals))
    print(results_dict)
    return results_dict

def nearest_date(dates, targdate):
    # given a pd series of dates and a target date, returns date from the series closest to target date (and distance)
    for i in dates:
        i = i.to_pydatetime()
    nearest = min(dates, key=lambda x: abs(x - targdate))
    timedelta = abs(nearest - targdate)
    return nearest, timedelta

def adjust_inflation(data, coeffs):
    most_recent = coeffs.iloc[-1]['CPIAUCNS']
    adjusted = data.reset_index().sort_values(by='Date')
    merged_df = pd.merge_asof(adjusted, coeffs, left_on='Date', right_on='DATE')
    # Normalize CPI with most recent CPI being 1.0
    coeffs['CPIAUCNS'] = coeffs['CPIAUCNS'].divide(most_recent)
    
    merged_df["IA Avg Price"] = (merged_df['Weighted Avg Price']/merged_df['CPIAUCNS'])
    merged_df = merged_df.set_index('Date')
    merged_df = merged_df.sort_index()
    print('Data with inflation index added:\n', merged_df.head())
    return merged_df

def load_cpi_data():
    coeffs = pd.read_csv('./CPI_DATA.csv')
    coeffs['DATE'] = pd.to_datetime(coeffs['DATE'])
    coeffs = coeffs.sort_values(by='DATE')
    coeffs = coeffs.reset_index(drop=True)
    return coeffs

def init_firestore():
    project_id = 'farmlink-304820'
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {
    'projectId': project_id,
    })

    return firestore.client()


if __name__ == "__main__":
    coeffs = load_cpi_data()
    db = init_firestore()

    for r in test_regions:
        for v in test_producenames:
            results_df = pd.DataFrame()
            input_df = update_data(r, v, test=True)
            adjusted_df = adjust_inflation(input_df, coeffs)
            result = calc_averages(r, v, adjusted_df,adjusted=True)
            print(result)
            # Next three lines save a backup, for now
            path = JSON_DIR + r + '_'+ v + '.json'
            with open(path, 'w') as fp:
                json.dump(result, fp)

            try:
                doc_ref = db.collection(u'farmlink_transactions').document(r)
            except:
                print("connecting to firestore failed for ", r)
                
            doc_ref.set(result)

                
    










