### Update Feb 20: ###
The script `test_script.py` was updated to scrape for new data for the current week. This data is then appended on the end of the rest of the data for that region & produce item, and the data older than 11 years back from the current date is dropped. The CSV file for the given produce item and region is overwritten with the new data, as a backup. The new data is then used for calculations and dumps a json object of the results in a file named REGION_PRODUCEITEM.json in the directory `json_data/`.
  
#### Still todo: ####
- Change to connect to firestore and dump the resulting json there
- Hang on to the json data from last week, just in case.
- Adjust for inflation
