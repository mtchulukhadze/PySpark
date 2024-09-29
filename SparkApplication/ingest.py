import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')
logger = logging.getLogger("ingest")

def load_files(spark, file_dir, file_type):
    try:
        logger.warning("load files...")

        if file_type == "parquet":
            dataframe = spark.read.format(file_type).load(file_dir)
        elif file_type == "csv":
            dataframe = spark.read.format(file_type).load(file_dir)

    except Exception as e:
        logger.error(e)

    else:
        print("DataFrame loaded")

    return dataframe


def display(df):
    df_show = df.show()
    return df_show
