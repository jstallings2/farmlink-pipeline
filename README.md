### Update Feb 20: ###
The script `test_script.py` was updated to scrape for new data for the current week. This data is then appended on the end of the rest of the data for that region & produce item, and the data older than 11 years back from the current date is dropped. The CSV file for the given produce item and region is overwritten with the new data, as a backup. The new data is then used for calculations and dumps a json object of the results in a file named REGION_PRODUCEITEM.json in the directory `json_data/`.
  
#### Still todo: ####
- Change to connect to firestore and dump the resulting json there
- Hang on to the json data from last week, just in case.
- Adjust for inflation
- Clean up the code a little bit to make it more readable

### Update Feb 21: ###
Added code to perform calculations using the inflation-adjusted prices if desired. Uses CPI data from https://fred.stlouisfed.org/series/CPIAUCNS (this data is updated monthly, might want to think about also automating a monthly scrape for the new CPI

#### Still todo: ####
- Change to connect to firestore and dump the resulting json there
- Add write/read rules to firestore (security)
- Hang on to the json data from last week, just in case.
- Optionally add a scrape for the newest CPI
- Clean up the code a little bit to make it more readable
