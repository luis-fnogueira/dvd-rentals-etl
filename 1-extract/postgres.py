import logging

import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Postgres:
    def __init__(self, credentials: dict) -> None:

        self.__CREDENTIALS = credentials

        self.URI = f"postgresql://{credentials['user']}:\
                    {credentials['password']}@{credentials['host']}\
                    :{credentials['port']}/{credentials['database']}"

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

    def execute_query(self, query: str, vars: str = "") -> None:
        """
        Abstraction of a query execution.
        Args:
            Query: str. A query to be run.
            Vars: str. Variables to be inserted.
        Return:
            None, it executes a query in a DB.

        """

        conn = self.get_conn()
        cur = conn.cursor()

        cur.execute(query=query, vars=(vars,))

        conn.commit()
        conn.close()
        cur.close()
