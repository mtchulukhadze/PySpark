import os

import get_env_variables as gev
from create_spark import get_spark_object
from validate import get_current_date
import logging.config
from ingest import load_files, display


logging.config.fileConfig('Properties/configuration/logging.config')

def main():
    global file_dir, file_format
    df = None
    try:
        logging.info('Starting Spark application')

        spark = get_spark_object(gev.appName)
        print("Spark Created", spark)

        get_current_date(spark)

        for file in os.listdir(gev.src_olap):
            print("file", file)

            file_dir = gev.src_olap + "\\" + file # file directory
            print(file_dir)

            if file.endswith(".parquet"):
                file_format = "parquet"

            elif file.endswith("csv"):
                file_format = "csv"

                df = load_files(spark=spark, file_dir=file_dir, file_type=file_format)

            else:
                file_format = os.path(file_dir.splitext(file))

            logging.info("reading file with format <{}".format(file_format))


        if df is not None:
            logging.info("run display functions")
            display(df)


    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()

