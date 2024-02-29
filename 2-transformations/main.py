import logging

import awswrangler as wr
import boto3
import duckdb
from dvd_wrangler import DVDWrangler

logger = logging.getLogger()
logging.basicConfig(filename="transformations.log", level=logging.INFO)


session = boto3.Session(profile_name="default")


paths = [
    "s3://dvd-rentals-datalake/bronze/rental/",
    "s3://dvd-rentals-datalake/bronze/staff/",
    "s3://dvd-rentals-datalake/bronze/country/",
    "s3://dvd-rentals-datalake/bronze/city/",
    "s3://dvd-rentals-datalake/bronze/address/",
    "s3://dvd-rentals-datalake/bronze/payment/",
    "s3://dvd-rentals-datalake/bronze/customer/",
]


dataframes_dict = DVDWrangler.read_parquet_files(paths, session)

rental = dataframes_dict["rental"]
staff = dataframes_dict["staff"]
country = dataframes_dict["country"]
city = dataframes_dict["city"]
address = dataframes_dict["address"]
payment = dataframes_dict["payment"]
customer = dataframes_dict["customer"]

query = """SELECT
            rental.rental_id,
            rental.rental_date,
            rental.customer_id,
            rental.return_date,
            rental.staff_id,
            address.city_id,
            city.country_id,
            address.address_id,
            customer.store_id,
            payment.payment_id,
            rental.last_update,
            payment.amount
            FROM rental
            INNER JOIN customer ON rental.customer_id = customer.customer_id
            INNER JOIN address ON customer.address_id = address.address_id
            INNER JOIN city ON address.city_id = city.city_id
            INNER JOIN country ON city.country_id = country.country_id
            INNER JOIN payment ON rental.customer_id = payment.customer_id;
        """

fact_rentals = duckdb.query(query).to_df()

logger.info("Fact table created")

# Loading fact table
wr.s3.to_parquet(
    df=fact_rentals,
    path="s3://dvd-rentals-datalake/silver/fact_rentals/",
    dataset=True,
    partition_cols=["rental_date"],
)

logger.info("Fact table loaded")

method_calls = [
    {
        "df": staff,
        "columns": ["address_id", "store_id"],
        "path": "s3://dvd-rentals-datalake/silver/staff/",
        "partition_cols": ["staff_id"],
    },
    {
        "df": city,
        "columns": ["country_id"],
        "path": "s3://dvd-rentals-datalake/silver/city/",
        "partition_cols": ["city_id"],
    },
    {
        "df": address,
        "columns": ["city_id"],
        "path": "s3://dvd-rentals-datalake/silver/address/",
        "partition_cols": ["address_id"],
    },
    {
        "df": payment,
        "columns": ["customer_id", "staff_id", "staff_id"],
        "path": "s3://dvd-rentals-datalake/silver/payment/",
        "partition_cols": ["payment_id"],
    },
    {
        "df": customer,
        "columns": ["address_id", "store_id"],
        "path": "s3://dvd-rentals-datalake/silver/customer/",
        "partition_cols": ["customer_id"],
    },
]

# Dropping unnecessary columns and loading them to S3
for call_info in method_calls:

    df = call_info["df"]
    columns = call_info["columns"]
    path = call_info["path"]
    partition_cols = call_info["partition_cols"]

    DVDWrangler.drop_columns(df=df, columns=columns)

    logger.info(f"Columns dropped in {path}")

    wr.s3.to_parquet(df=df, path=path, dataset=True, partition_cols=partition_cols)

    logger.info(f"{path} loaded")
