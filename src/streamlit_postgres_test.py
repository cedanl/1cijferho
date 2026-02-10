import pandas as pd
import psycopg2
from psycopg2 import sql
import sys

def get_table_from_postgres(host, dbname, user, password, table_name):
    conn = psycopg2.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password
    )
    query = sql.SQL("SELECT * FROM {};").format(sql.Identifier(table_name))
    query_str = query.as_string(conn)
    df = pd.read_sql_query(query_str, conn)
    conn.close()
    return df

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch a table from PostgreSQL and print to console.")
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--dbname', default='postgres', help='Database name')
    parser.add_argument('--user', default='postgres', help='User name')
    parser.add_argument('--password', required=True, help='Password')
    parser.add_argument('--table', required=True, help='Table name')
    args = parser.parse_args()

    try:
        df = get_table_from_postgres(args.host, args.dbname, args.user, args.password, args.table)
        print(df.to_string(index=False))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
