import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_db():
    """
    -   Create sparkifydb
    -   Connect to sparkifydb
    """

    # Connect to default db
    try:
        conn = psycopg2.connect("host=localhost user=postgres password=uncle1dee")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")\


    # Create the cursor
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the database")
        print(e)

    conn.set_session(autocommit=True)

    # Create sparkify db UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE  template0")
    except psycopg2.Error as e:
        print("Error: Could not create the database")
        print(e)

    # close connection to default database
    conn.close()

    # Connect to sparkifydb
    try:
        conn = psycopg2.connect("host=localhost dbname=sparkifydb user=postgres password=uncle1dee")
    except psycopg2.Error as e:
        print("Error: Could not make connection to the sparkifydb database")
        print(e)

    # Create the cursor
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the database")
        print(e)

    return cur, conn


create_db()

