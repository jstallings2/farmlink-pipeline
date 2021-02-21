import requests
import os
import time
import random
import sys
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta 

def get_current_date():
    today = datetime.now()
    return today.month, today.day, today.year


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
            sys.exit()


def load_and_clean(region, veg, dir='./concat_data/'):
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

def yearsago(years, from_date=None):
    if from_date is None:
        from_date = datetime.now()
    return from_date - relativedelta(years=years)

def update_data(regionname, producename, savedir='./concat_data/'):
    """
    Performs the web scrape to grab the newest data for a given region and veggie,
    and combines it with the old data for that region & veggie and returns the
    combined dataframe with data +11 years ago cut off, and also OVERWRITES the backup csv of the old data
    """

    new_df = fetch_data('APPLES', 'NORTHEAST+U.S.')[0]
    old_df = load_and_clean('NORTHEAST+U.S.', 'APPLES')[0]
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

if __name__ == "__main__":
    print(update_data('NORTHEAST+U.S.', 'APPLES').tail(20))


