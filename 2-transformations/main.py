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

wr.s3.to_parquet(
    df=fact_rentals,
    path="s3://dvd-rentals-datalake/silver/fact_rentals/",
    dataset=True,
    partition_cols=["rental_date"],
)
