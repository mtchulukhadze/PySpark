
import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
loggers = logging.getLogger("Validate")

def get_current_date(spark):
    try:
        loggers.warning("started the current date")
        spark = spark.sql("""select current_date""")
        loggers.warning("validate spark object, Date", str(spark.collect()))


    except Exception as e:
        loggers.error(str(e))