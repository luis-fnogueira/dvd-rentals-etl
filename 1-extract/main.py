from postgres import Postgres

credentials = {
    "host": "localhost",
    "port": "5432",
    "user": "admin",
    "password": "admin",
    "database": "dvdrental",
}

ps = Postgres(credentials=credentials)

actor = ps.get_data(table="actor", schema="public")
