### Update Mar 24: ###
See branch: `cloudStorage`  
Haven't written an update in a minute but here's where we stand after the last push:
- Script works by scraping a week and reading the csv of the older data from Cloud Storage. FWIW after the first read the type of the file in cloud storage is changed from `text/csv` to `application/octet`. Don't know if this would be an issue but should be fine as long as the csv's are getting correctly updated each week. We'll know next week if the code scraped from 3/19 is in there along with the code from 3/27.
- `main.py` is now the exact same code as `test_script.py`. Changes to the production code should be documented, changed in this `main.py` in the repo, comitted, THEN copy-pasted to the GCF inline editor.
- A function `get_latest_cpi()` was added to the script that sends a simple GET request to the Federal Reserve in St. Louis's server for the latest version of the CPI data that we use to adjust for inflation. If there is a server error, the script just uses the last version of the csv that was saved (this file lives in local cloud function storage on GCP, next to `requirements.txt`, etc. and will likely stay there in production, I don't see any need to store it elsewhere)

#### Still todo: ####
- It's time to go ahead and generalize, I will scrape manually for all veggies & regions on my local machine and manually upload the csv's I get to cloud storage.
- After that the cloud function code should be changed to generalize for all veggies & regions.
- There might be a delay between when the data is collected and when it is actually published on the USDA server. For example even though the date is Friday, the server might not be updated for a while (even the next 48 hrs) which would result in us not having the most recent data if we run the scrape on Friday before the data is actually posted. If this is the case, we might want to look into changing the Pub/Sub to be in the middle of the week, so that we give the server ample time to be updated with the latest Friday data.




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
