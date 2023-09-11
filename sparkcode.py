from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def process_data():
    spark = SparkSession.builder.appName("GCPDataprocJob").enableHiveSupport().getOrCreate()	
    bucket = "stock-extracted-data-bucket"
    top_gainers = f"gs://{bucket}/landing/top-gainers/newfile/"
    output_path = f"gs://{bucket}/output.csv"
    schema='c_name string,ltp float,change float,pChange float'
    topg1 = spark.read.format("json").option("multiline","true").schema(schema).load(top_gainers)
    topg2 = topg1.withColumn("Date",current_date())
    topg3 = topg2.distinct()
    topg3.write.format("parquet").option("header","true").mode("append").saveAsTable("project.stockg")
    spark.stop()
if __name__ == "__main__":
    process_data()