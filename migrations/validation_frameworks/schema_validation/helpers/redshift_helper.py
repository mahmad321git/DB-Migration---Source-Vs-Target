
import psycopg2


def get_connection(target_dbname, target_host, target_port, target_username, target_password):
    con = psycopg2.connect(dbname=target_dbname,
                     host=target_host,
                     port=target_port,
                     user=target_username,
                     password=target_password)
    return con


class redshift_helper:

    def query_redshift(self, query, target_dbname, target_host, target_port, target_username, target_password):
        con = get_connection(target_dbname, target_host, target_port, target_username, target_password)
        cur = con.cursor()
        cur.execute(query)
        result = cur.fetchall()
        return result

    def insert_row(self, query, target_dbname, target_host, target_port, target_username, target_password):
        con = get_connection(target_dbname, target_host, target_port, target_username, target_password)
        con.autocommit = True
        cur = con.cursor()
        cur.execute(query)

    def query_pg(self, query, target_dbname, target_host, target_port, target_username, target_password):
        con = get_connection(target_dbname, target_host, target_port, target_username, target_password)
        con.autocommit = True
        cur = con.cursor()
        cur.execute(query)
        result = cur.fetchall()
        return result

    def execute_query(self, query, target_dbname, target_host, target_port, target_username, target_password):
        con = get_connection(target_dbname, target_host, target_port, target_username, target_password)
        con.autocommit = True
        cur = con.cursor()
        cur.execute(query)