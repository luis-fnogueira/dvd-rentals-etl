# adding transformations to the system path
import logging
import sys

import awswrangler as wr
import boto3
from dvd_wrangler import DVDWrangler

sys.path.insert(0, "/home/luis/code/dvd-rental-modeling/transformations")

logger = logging.getLogger()
logging.basicConfig(filename="loading.log", level=logging.INFO)


session = boto3.Session(profile_name="default")


paths = [
    "s3://dvd-rentals-datalake/silver/fact_rentals/",
    "s3://dvd-rentals-datalake/silver/staff/",
    "s3://dvd-rentals-datalake/silver/country/",
    "s3://dvd-rentals-datalake/silver/city/",
    "s3://dvd-rentals-datalake/silver/address/",
    "s3://dvd-rentals-datalake/silver/payment/",
    "s3://dvd-rentals-datalake/silver/customer/",
]


dataframes_dict = DVDWrangler.read_parquet_files(paths, session)

fact_rentals = dataframes_dict["fact_rentals"]
staff = dataframes_dict["staff"]
country = dataframes_dict["country"]
city = dataframes_dict["city"]
address = dataframes_dict["address"]
payment = dataframes_dict["payment"]
customer = dataframes_dict["customer"]


method_calls = [
    {
        "df": staff,
        "table": "dim_staff",
        "path": "s3://dvd-rentals-datalake/gold/staff/",
    },
    {"df": city, "table": "dim_city", "path": "s3://dvd-rentals-datalake/gold/city/"},
    {
        "df": address,
        "table": "dim_address",
        "path": "s3://dvd-rentals-datalake/gold/address/",
    },
    {
        "df": payment,
        "table": "dim_payment",
        "path": "s3://dvd-rentals-datalake/gold/payment/",
    },
    {
        "df": country,
        "table": "dim_country",
        "path": "s3://dvd-rentals-datalake/gold/country/",
    },
    {
        "df": customer,
        "table": "dim_customer",
        "path": "s3://dvd-rentals-datalake/gold/customer/",
    },
    {
        "df": fact_rentals,
        "table": "fact_rentals",
        "path": "s3://dvd-rentals-datalake/gold/fact_rentals/",
    },
]


for call_info in method_calls:

    df = call_info["df"]
    table = call_info["table"]
    path = call_info["path"]

    wr.athena.to_iceberg(
        df=df,
        database="gold_dimensional_dvd_rental",
        table_location=path,
        table=table,
        temp_path="s3://dvd-rentals-datalake/temp-path/",
    )

    logger.info(f"{table} loaded")
