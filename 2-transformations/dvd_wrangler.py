import logging

import awswrangler as wr
import boto3
import pandas as pd

logger = logging.getLogger()
logging.basicConfig(filename="DVDwrangler.log", level=logging.INFO)


class DVDWrangler:

    @staticmethod
    def drop_columns(df: pd.DataFrame, columns: list) -> pd.DataFrame:
        """_summary_

        Args:
            df (pd.DataFrame): _description_
            columns (list): _description_

        Returns:
            pd.DataFrame: _description_
        """

        df = df.drop(columns, axis=1)

        return df

    @staticmethod
    def read_parquet_files(paths: list, session: boto3.Session) -> dict:

        dataframes = {}

        for path in paths:
            table_name = path.split("/")[-2]  # Extract the table name from the path
            dataframes[table_name] = wr.s3.read_parquet(
                path=path, boto3_session=session
            )

            logger.info(msg=f"{table_name} read from S3")

        return dataframes
