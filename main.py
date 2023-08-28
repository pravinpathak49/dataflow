from utils.logger import logging
from pyspark.sql import SparkSession


def get_spark():
    try:
        spark = SparkSession.builder.appName('My_Spark_Session').getOrCreate()
        return spark
    except Exception as e:
        logging.error(f'Issue in creating Spark Session {e}')


if __name__ == "__main__":
    logging.info("Generating spark Session")
    spark = get_spark()
    logging.info(f'Spark session generated {spark}')