import logging

import awswrangler as wr
import boto3
from postgres import Postgres

logger = logging.getLogger()
logging.basicConfig(filename="extract.log", level=logging.INFO)


# I am hardcoding the credentials here but in a production environment I would
# use secrets manager
CREDENTIALS = {
    "host": "localhost",
    "port": "5432",
    "user": "admin",
    "password": "admin",
    "database": "dvdrental",
}

br_path = "s3://dvd-rentals-datalake/bronze/"
ps = Postgres(credentials=CREDENTIALS)
session = boto3.Session(profile_name="default")

tables = ["country", "city", "address", "customer", "staff", "rental", "payment"]


for table in tables:

    df = ps.get_data(table=table)
    wr.s3.to_parquet(
        df=df, path=br_path + table + "/", boto3_session=session, dataset=True
    )

    logger.info(f"{table} inserted at {br_path + table + '/'}")
