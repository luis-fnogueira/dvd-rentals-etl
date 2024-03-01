import logging

import awswrangler as wr
import boto3

from transformations.dvd_wrangler import DVDWrangler

logger = logging.getLogger()
logging.basicConfig(filename="loading.log", level=logging.INFO)


session = boto3.Session(profile_name="default")


paths = [
    "s3://dvd-rentals-datalake/silver/rental/",
    "s3://dvd-rentals-datalake/silver/staff/",
    "s3://dvd-rentals-datalake/silver/country/",
    "s3://dvd-rentals-datalake/silver/city/",
    "s3://dvd-rentals-datalake/silver/address/",
    "s3://dvd-rentals-datalake/silver/payment/",
    "s3://dvd-rentals-datalake/silver/customer/",
]


dataframes_dict = DVDWrangler.read_parquet_files(paths, session)

rental = dataframes_dict["rental"]
staff = dataframes_dict["staff"]
country = dataframes_dict["country"]
city = dataframes_dict["city"]
address = dataframes_dict["address"]
payment = dataframes_dict["payment"]
customer = dataframes_dict["customer"]


method_calls = [
    {"df": staff, "table": "dim_staff", "columns": ["address_id", "store_id"]},
    {"df": city, "table": "dim_city", "columns": ["country_id"]},
    {"df": address, "table": "dim_address", "columns": ["city_id"]},
    {
        "df": payment,
        "table": "dim_payment",
        "columns": ["customer_id", "staff_id", "staff_id"],
    },
    {"df": customer, "table": "dim_customer", "columns": ["address_id", "store_id"]},
]


for call_info in method_calls:

    df = call_info["df"]
    table = call_info["table"]

    wr.athena.to_iceberg(df=df, database="gold_dimensional_dvd_rental", table=table)

    logger.info(f"{table} loaded")
