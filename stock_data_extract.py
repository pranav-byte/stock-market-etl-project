from bsedata.bse import BSE
from google.cloud import storage
from datetime import datetime
import json
import os

bse = BSE()
topgainers = bse.topGainers()
day_off_check = bse.getQuote('540376')

today_date=datetime.now()
date=today_date.date()

if int(day_off_check['updatedOn'][0:2])==date.day:

    file_name=f'dailytopgain-{date.year}{date.month}{date.day}'
    file_path="/home/pranavyadavbricks/raw_data"
    with open(file_name,'w') as jfile:
        json.dump(topgainers,jfile,indent=2)

    os.system(f"gsutil cp {file_name} gs://stock-extracted-data-bucket/landing/top-gainers/newfile/")
    os.system(f"mv {file_name} {file_path}")