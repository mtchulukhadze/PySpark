
from pyspark.sql import SparkSession
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger("Create_Spark")

def get_spark_object(appName):
    try:
        logger.info("get-spark_object")

        if appName == 'PySkark':
            appName == 'PySkark'
        else:
            appName == 'Yarn'

        logger.info("get-spark_object".format(appName))

        spark = SparkSession.builder.appName(appName).getOrCreate()
        return spark

    except Exception as e:
        logger.error(e)


