from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.functions import col,split,count
import logging

#____________LOGGING SETUP_____________#

logging.basicConfig(filename="/home/rayaan/incremental_log_data_pipeline/logs/pipeline.log",level=logging.DEBUG,format="%(asctime)s-%(levelname)s-%(message)s")

#____________PATHS_______________#
data_folder_path="/home/rayaan/incremental_log_data_pipeline/data"
processed_file_path="/home/rayaan/incremental_log_data_pipeline/processed_files.txt"

#____________PIPELINE_____________#
try:
    logging.info("==========Pipeline Execution Started==========")
    all_files = set(os.listdir(data_folder_path))
    logging.debug(f"All files:{all_files}")

    try:
        with open(processed_file_path, "r") as files:
            processed_files = set(files.read().splitlines())
    except FileNotFoundError:
        logging.warning("processed file not found, starting fresh")
        processed_files = set()
    logging.debug(f"Processed files:{processed_files}")

    new_files = list(all_files - processed_files)
    logging.debug(f"New files:{new_files}")

    if not new_files:
        logging.warning("No new files to process")
        sys.exit(0)

    logging.info(f"New files to process:{new_files}")

    spark = SparkSession.builder\
        .appName("Log Data Pipeline")\
        .master("local[*]")\
        .config("spark.hadoop.fs.defaultFS","file:///")\
        .config("spark.sql.shuffle.partitions","4")\
        .getOrCreate()

    logging.info("Spark Session Created")

    new_file_path = [os.path.join(data_folder_path, file) for file in new_files]
    logging.debug(f"New file paths:{new_file_path}")

    df = spark.read.text(new_file_path)
    logging.info("Files read into DataFrame")

    split_col = split(col("value"), " ")

    df_parsed = df.select(split_col.getItem(0).alias("date"),
                          split_col.getItem(1).alias("time"),
                          split_col.getItem(2).alias("log_level"),
                          split_col.getItem(3).alias("user_id"),
                          split_col.getItem(4).alias("action"))

    logging.info("Parsing completed")

    #____________JDBC_____________#
    url = "jdbc:mysql://localhost:3306/log_pipeline_db"
    driver = "com.mysql.cj.jdbc.Driver"
    password = os.getenv("db_password")

    if not password:
        raise ValueError("db_password not set")

    users_df = spark.read.format("jdbc").option("url", url).option("dbtable", "users").option("user", "root").option(
        "password", password).option("driver", driver).load()

    logging.info("JDBC read successful")

    #______JOIN______#
    joined_df = df_parsed.join(users_df, "user_id")
    joined_df.cache()
    joined_df.count()

    logging.info("Join completed")

    if joined_df.limit(1).count()==0:
        logging.warning("Join resulted in empty dataset")

    #_______FILTER_______#
    filtered_df = joined_df.filter(col("log_level") == "ERROR")
    logging.info("Filtering Error logs completed")

    if filtered_df.limit(1).count()==0:
        logging.warning("No ERROR logs found")

    #_______AGGREGATION__________#
    error_count_per_day = filtered_df.groupBy("date").agg(count("*").alias("error_count_per_day")).orderBy(
        "error_count_per_day", ascending=False)

    error_count_per_city = filtered_df.groupBy("city").agg(count("*").alias("error_count_per_city")).orderBy(
        "error_count_per_city", ascending=False)

    logging.info("Aggregation completed")

    #___________WRITE______________#
    error_count_per_day.write.mode("append").partitionBy("date").parquet(
        "file:///home/rayaan/incremental_log_data_pipeline/output/error_count_per_day")

    error_count_per_city.write.mode("append").partitionBy("city").parquet(
        "file:///home/rayaan/incremental_log_data_pipeline/output/error_count_per_city")

    logging.info("Data written Successfully")

    logging.info("Stopping Spark Session")
    spark.stop()


    #__________UPDATE TRACKER______________#
    with open(processed_file_path, 'a') as file:
        for files in new_files:
            file.write(files + "\n")
    logging.info("Processed files updated")
    logging.info("==========Pipeline Completed Successfully==========")

#_____________ERROR HANDLING_____________#
except Exception as e:
    logging.error(f"Pipeline Failed:{e}",exc_info=True)
    sys.exit(1)
