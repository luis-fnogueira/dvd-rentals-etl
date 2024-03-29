import logging

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Postgres:
    def __init__(self, credentials: dict) -> None:

        self.__CREDENTIALS = credentials

        self.URI = (
            f"postgresql://{credentials['user']}:{credentials['password']}@"
            f"{credentials['host']}:{credentials['port']}"
            f"/{credentials['database']}"
        )

    def get_conn(self):

        try:

            return psycopg2.connect(
                host=self.__CREDENTIALS["host"],
                port=self.__CREDENTIALS["port"],
                user=self.__CREDENTIALS["user"],
                password=self.__CREDENTIALS["password"],
                database=self.__CREDENTIALS["database"],
            )

        except Exception as error:

            logger.error(error)

    def get_data(self, table: str) -> pd.DataFrame:
        """
        This function reads data from postgres and creates a pandas df from it.
        Args:
            Table: str. The table where to get data from.
        Return:
            Pandas Dataframe, it executes a query in a DB and return the data
            as a DF.
        """

        conn = self.get_conn()
        cur = conn.cursor()

        alchemy_engine = create_engine(self.URI)
        db_conn = alchemy_engine.connect()

        df = pd.read_sql_table(table_name=table, con=db_conn)

        conn.commit()
        conn.close()
        cur.close()

        logger.info(f"Dataframe created from table {table}")
        return df
