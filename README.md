# stock-market-etl-project
1.stock_data_extract.py - extracts stock market top gainers data from BSE stock exchange library and uploads it to the landing folder of the bucket in the GCP

2.file_check.py - Airflow dag to check if the file is uploaded in the landing folder.

3.spark_submit_job.py - Airflow dag to submit pyspark code to the dataproc cluster in GCP.

4.sparkcode.py - This file contains the actual pyspark transformation which is used by airflow dag to run on the spark cluster.

![Screenshot 2023-09-10 172428](https://github.com/pranav-byte/stock-market-etl-project/assets/51847389/426bab8a-48e1-451d-ab12-1f04af60672f)
