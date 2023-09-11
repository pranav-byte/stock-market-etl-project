# stock-market-etl-project
1.file_check.py - Airflow dag to check if the file is uploaded in the landing folder.

2.spark_submit_job.py - Airflow dag to submit pyspark code to the dataproc cluster in GCP.

3.sparkcode.py - This file contains the actual pyspark transformation which is used by airflow dag to run on the spark cluster.
