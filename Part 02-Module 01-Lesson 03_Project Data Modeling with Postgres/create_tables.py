import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_db():
    """
    -   Create sparkifydb
    -   Connect to sparkifydb
    """

    try:
        conn = psycopg2.connect("host=localhost user=postgres password=uncle1dee")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the database")
        print(e)

    conn.set_session(autocommit=True)


create_db()

