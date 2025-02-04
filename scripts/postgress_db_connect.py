import psycopg2
DB_CONFIG = {
    "host": "localhost",
    "port": "15432",
    "dbname": "postgres",
    "user": "postgres",
    "password": "analytics@123",
    "driver" : "org.postgresql.Driver"
}

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://localhost:5432/postgres"
# postgres_properties = {
#     "user": "postgres",
#     "password": "analytics@123",
#     "driver": "org.postgresql.Driver"
# }


def get_db_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )
